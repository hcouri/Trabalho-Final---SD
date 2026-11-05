"""
Treinamento, Validação e Persistência do Pipeline de Machine Learning (`pyspark.ml`)
para Reconhecimento de Padrões e Detecção de Anomalias Pontuais ou Iminentes (Pessoa 3).

Este módulo cumpre a Tarefa 3 da Pessoa 3:
"Implementar e integrar o algoritmo de Machine Learning no Spark (pyspark.ml) para
reconhecimento de padrões e detecção de anomalias pontuais ou iminentes."

Arquitetura do `pyspark.ml.Pipeline`:
  1. `Imputer`: Imputação por mediana para proteção contra valores ausentes residuais.
  2. `VectorAssembler`: Agrupa as 14 features (8 sensores normalizados + 4 indicadores
     estatísticos Z-Score + 2 diferenciais termohidráulicos) no vetor `features_raw`.
  3. `StandardScaler`: Padronização vetorial (`withMean=True`, `withStd=True`) ajustada
     exclusivamente nos dados de treino (evitando data leakage).
  4. `KMeans` (k=4): Algoritmo não-supervisionado para reconhecimento de padrões e
     regimes operacionais do poço (`cluster_padrao`).
  5. `RandomForestClassifier` (60 árvores): Algoritmo supervisionado multiclasse para
     detecção de anomalias pontuais ou iminentes:
       - Label `0.0` -> `NORMAL` (Operação normal do poço)
       - Label `1.0` -> `ANOMALIA_IMINENTE` (Regime transiente / falha incipiente: classes 101..109)
       - Label `2.0` -> `ANOMALIA_PONTUAL` (Falha severa consolidada: classes 1..9)
"""

import json
import random
from pathlib import Path
from typing import Tuple, Dict, Any
import pandas as pd  # pyrefly: ignore[missing-import]

from pyspark.sql import SparkSession, DataFrame  # pyrefly: ignore[missing-import]
from pyspark.sql import functions as F  # pyrefly: ignore[missing-import]
from pyspark.ml import Pipeline, PipelineModel  # pyrefly: ignore[missing-import]
from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler  # pyrefly: ignore[missing-import]
from pyspark.ml.clustering import KMeans  # pyrefly: ignore[missing-import]
from pyspark.ml.classification import RandomForestClassifier  # pyrefly: ignore[missing-import]
from pyspark.ml.evaluation import MulticlassClassificationEvaluator  # pyrefly: ignore[missing-import]

from src.streaming.preprocessamento import (
    SENSORES_CANONICOS,
    COLUNAS_FEATURES_ML,
    carregar_parametros_estatisticos,
    aplicar_preprocessamento_online,
)

# Diretórios de persistência do modelo treinado e do relatório de métricas (usado também pela Pessoa 4)
CAMINHO_MODELO_PADRAO = Path("models/pipeline_ml_3w")
CAMINHO_METRICAS_PADRAO = Path("models/metricas_modelo.json")


def mapear_classe_para_label_estado(col_classe: F.Column, col_z_max: F.Column) -> F.Column:
    """
    Converte o código `class` do Dataset 3W no rótulo alvo (`label_estado`) do classificador:
      - 0.0: Operação Normal (class == 0 e z_score_max < 2.2)
      - 1.0: Anomalia Iminente / Transiente (class >= 100 no 3W ou desvio moderado 2.2 <= z < 3.2)
      - 2.0: Anomalia Pontual / Crítica (1 <= class <= 99 no 3W ou desvio extremo >= 3.2)
    """
    return (
        F.when((col_classe >= 1) & (col_classe <= 99), F.lit(2.0))
        .when(col_classe >= 100, F.lit(1.0))
        .when(col_z_max >= 3.2, F.lit(2.0))
        .when(col_z_max >= 2.2, F.lit(1.0))
        .otherwise(F.lit(0.0))
    )


def _gerar_dataset_calibracao_3w(
    spark: SparkSession, parametros: Dict[str, Any], n_amostras: int = 3000
) -> DataFrame:
    """
    Gera um conjunto de calibração sintético fiel às distribuições físicas do Dataset 3W
    (a partir das médias e desvios do Trabalho 2) caso a pasta `data/raw` não esteja
    presente na máquina local, permitindo treinar e validar o modelo autonomamente.
    """
    rng = random.Random(42)
    cfg_sensores = parametros["sensores"]
    registros = []

    classes_iminentes = [101, 102, 103, 104, 105, 106, 107, 108]
    classes_pontuais = [1, 2, 3, 4, 5, 6, 7, 8]

    for i in range(n_amostras):
        u = rng.random()
        if u < 0.55:
            # 55% Operação Normal (oscilação normal dentro de ~0.65 sigma)
            classe = 0
            fator_desvio = rng.gauss(0.0, 0.65)
            escala_ruido = 0.65
        elif u < 0.80:
            # 25% Anomalia Iminente / Transiente (deriva incipiente entre 1.85 e 2.85 sigmas)
            classe = rng.choice(classes_iminentes)
            sinal = rng.choice([-1.0, 1.0])
            fator_desvio = sinal * rng.uniform(1.85, 2.85)
            escala_ruido = 0.55
        else:
            # 20% Anomalia Pontual Crítica (falha severa entre 3.05 e 5.50 sigmas)
            classe = rng.choice(classes_pontuais)
            sinal = rng.choice([-1.0, 1.0])
            fator_desvio = sinal * rng.uniform(3.05, 5.50)
            escala_ruido = 0.75

        linha = {
            "poco_id": f"WELL-0000{(i % 5) + 1}",
            "arquivo_origem": f"WELL-0000{(i % 5) + 1}_CALIBRACAO.parquet",
            "classe_real": int(classe),
        }

        # Seleciona de 2 a 4 sensores principais sensibilizados pelo evento anômalo
        sensores_afetados = set(rng.sample(SENSORES_CANONICOS, k=rng.randint(2, 4)))

        for sensor in SENSORES_CANONICOS:
            media = float(cfg_sensores[sensor]["media"])
            desvio = float(cfg_sensores[sensor]["desvio_padrao"])

            # Simula 5% de falhas de leitura (sensores nulos, comum no Dataset 3W)
            if rng.random() < 0.05:
                linha[sensor] = None
                continue

            if classe == 0 or sensor not in sensores_afetados:
                z_simulado = rng.gauss(0.0, escala_ruido)
            else:
                z_simulado = fator_desvio + rng.gauss(0.0, 0.35)

            linha[sensor] = float(media + z_simulado * desvio)

        registros.append(linha)

    df_pandas = pd.DataFrame(registros)
    return spark.createDataFrame(df_pandas)


def carregar_dados_treino(
    spark: SparkSession,
    diretorio_raw: str = "data/raw",
    max_arquivos: int = 40,
    max_linhas_por_arquivo: int = 500,
) -> DataFrame:
    """
    Carrega amostras dos arquivos Parquet do Dataset 3W em lotes controlados (padrão
    de eficiência de memória do Trabalho 2) ou utiliza o gerador de calibração 3W
    caso a pasta `data/raw` esteja vazia.
    """
    parametros = carregar_parametros_estatisticos()
    path_raw = Path(diretorio_raw)
    arquivos = list(path_raw.glob("**/*.parquet")) if path_raw.exists() else []

    if not arquivos:
        print(
            f"[ML] Nenhum Parquet encontrado em '{path_raw}'. "
            "Gerando dataset de calibração estatística 3W (3.000 registros)..."
        )
        df_base = _gerar_dataset_calibracao_3w(spark, parametros, n_amostras=3000)
    else:
        print(
            f"[ML] Lendo até {min(len(arquivos), max_arquivos)} arquivos Parquet de '{path_raw}' "
            "com amostragem em lote..."
        )
        frames = []
        for arq in arquivos[:max_arquivos]:
            try:
                pdf = pd.read_parquet(arq)
                if len(pdf) > max_linhas_por_arquivo:
                    pdf = pdf.sample(n=max_linhas_por_arquivo, random_state=42)

                poco_id = arq.name.split("_")[0] if "_" in arq.name else "WELL-3W"
                pdf["poco_id"] = poco_id
                pdf["arquivo_origem"] = arq.name
                pdf["classe_real"] = (
                    pd.to_numeric(pdf["class"], errors="coerce").fillna(0).astype(int)
                    if "class" in pdf.columns
                    else 0
                )

                for sensor in SENSORES_CANONICOS:
                    if sensor not in pdf.columns:
                        pdf[sensor] = None
                    else:
                        pdf[sensor] = pd.to_numeric(pdf[sensor], errors="coerce")

                colunas_manter = ["poco_id", "arquivo_origem", "classe_real"] + SENSORES_CANONICOS
                frames.append(pdf[colunas_manter])
            except Exception as exc:
                print(f"[AVISO] Falha ao ler {arq.name}: {exc}")

        if not frames:
            df_base = _gerar_dataset_calibracao_3w(spark, parametros, n_amostras=3000)
        else:
            df_unificado = pd.concat(frames, ignore_index=True)
            df_spark_raw = spark.createDataFrame(df_unificado)
            # Complementa com amostras calibradas para garantir cobertura das 3 classes no treino
            df_calib = _gerar_dataset_calibracao_3w(spark, parametros, n_amostras=1200)
            df_base = df_spark_raw.unionByName(df_calib, allowMissingColumns=True)

    # Aplica a camada de pré-processamento e normalização online do Trabalho 2
    df_prep = aplicar_preprocessamento_online(df_base, parametros)

    # Cria a coluna alvo 'label_estado' (0.0 = Normal, 1.0 = Iminente, 2.0 = Pontual)
    df_rotulado = df_prep.withColumn(
        "label_estado",
        mapear_classe_para_label_estado(F.col("classe_real"), F.col("z_score_max")),
    )

    return df_rotulado


def construir_pipeline_ml() -> Pipeline:
    """
    Constrói o `pyspark.ml.Pipeline` integrando pré-processamento vetorial,
    clusterização para reconhecimento de padrões (KMeans) e classificação
    supervisionada para detecção de anomalias (RandomForestClassifier).
    """
    colunas_imputadas = [f"{c}_imp_ml" for c in COLUNAS_FEATURES_ML]

    # Estágio 1: Imputer para garantir ausência total de NaNs/Nulls no vetor de entrada
    imputer = Imputer(
        strategy="median",
        inputCols=COLUNAS_FEATURES_ML,
        outputCols=colunas_imputadas,
    )

    # Estágio 2: Montagem do vetor de features (14 dimensões)
    assembler = VectorAssembler(
        inputCols=colunas_imputadas,
        outputCol="features_raw",
        handleInvalid="keep",
    )

    # Estágio 3: Padronização das features ajustada no conjunto de treinamento
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True,
    )

    # Estágio 4: KMeans para identificar o cluster/regime operacional do poço
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster_padrao",
        k=4,
        seed=42,
        maxIter=30,
    )

    # Estágio 5: RandomForestClassifier para classificar Normal (0), Iminente (1) e Pontual (2)
    rf_classifier = RandomForestClassifier(
        featuresCol="features",
        labelCol="label_estado",
        predictionCol="predicao_ml",
        probabilityCol="probabilidade_ml",
        rawPredictionCol="raw_prediction_ml",
        numTrees=60,
        maxDepth=8,
        seed=42,
    )

    return Pipeline(stages=[imputer, assembler, scaler, kmeans, rf_classifier])


def treinar_e_salvar_modelo(
    spark: SparkSession,
    diretorio_raw: str = "data/raw",
    caminho_modelo: Path = CAMINHO_MODELO_PADRAO,
    caminho_metricas: Path = CAMINHO_METRICAS_PADRAO,
) -> Tuple[PipelineModel, Dict[str, Any]]:
    """
    Realiza a divisão estrita Treino/Teste (80%/20%) ANTES do ajuste do pipeline,
    treina o `pyspark.ml.Pipeline`, calcula métricas de validação e persiste o modelo
    e o arquivo JSON de métricas (para uso imediato no relatório técnico da Pessoa 4).
    """
    df_dados = carregar_dados_treino(spark, diretorio_raw=diretorio_raw)

    # Split 80/20 realizado ANTES do fit() para evitar vazamento de dados (data leakage)
    df_treino, df_teste = df_dados.randomSplit([0.8, 0.2], seed=42)

    print("[ML] Ajustando Pipeline pyspark.ml (Imputer + VectorAssembler + StandardScaler + KMeans + RandomForest)...")
    pipeline = construir_pipeline_ml()
    modelo_treinado = pipeline.fit(df_treino)

    print("[ML] Avaliando desempenho preditivo no conjunto de teste (20% hold-out)...")
    predicoes_teste = modelo_treinado.transform(df_teste)

    # Avaliadores oficiais do pyspark.ml.evaluation
    avaliador_acc = MulticlassClassificationEvaluator(
        labelCol="label_estado", predictionCol="predicao_ml", metricName="accuracy"
    )
    avaliador_f1 = MulticlassClassificationEvaluator(
        labelCol="label_estado", predictionCol="predicao_ml", metricName="f1"
    )
    avaliador_prec = MulticlassClassificationEvaluator(
        labelCol="label_estado", predictionCol="predicao_ml", metricName="weightedPrecision"
    )
    avaliador_rec = MulticlassClassificationEvaluator(
        labelCol="label_estado", predictionCol="predicao_ml", metricName="weightedRecall"
    )

    acuracia = float(avaliador_acc.evaluate(predicoes_teste))
    f1_score = float(avaliador_f1.evaluate(predicoes_teste))
    precisao = float(avaliador_prec.evaluate(predicoes_teste))
    recall = float(avaliador_rec.evaluate(predicoes_teste))

    # Gera matriz de confusão (Label Real vs Predição ML)
    matriz_rows = (
        predicoes_teste.groupBy("label_estado", "predicao_ml")
        .count()
        .orderBy("label_estado", "predicao_ml")
        .collect()
    )
    matriz_confusao = [
        {
            "label_real": int(r["label_estado"]),
            "predicao": int(r["predicao_ml"]),
            "contagem": int(r["count"]),
        }
        for r in matriz_rows
    ]

    metricas = {
        "algoritmo": "RandomForestClassifier (numTrees=60, maxDepth=8) + KMeans (k=4)",
        "features_utilizadas": COLUNAS_FEATURES_ML,
        "total_treino": df_treino.count(),
        "total_teste": df_teste.count(),
        "accuracy": round(acuracia, 4),
        "f1_score": round(f1_score, 4),
        "weighted_precision": round(precisao, 4),
        "weighted_recall": round(recall, 4),
        "matriz_confusao": matriz_confusao,
    }

    print("=" * 60)
    print(" MÉTRICAS DE VALIDAÇÃO DO MODELO PYSPARK.ML (PESSOA 3)")
    print("=" * 60)
    print(f" Acurácia (Accuracy)          : {metricas['accuracy'] * 100:.2f}%")
    print(f" F1-Score Ponderado           : {metricas['f1_score'] * 100:.2f}%")
    print(f" Precisão Ponderada           : {metricas['weighted_precision'] * 100:.2f}%")
    print(f" Recall Ponderado             : {metricas['weighted_recall'] * 100:.2f}%")
    print("=" * 60)

    # Salva o PipelineModel para carregamento rápido no Structured Streaming
    caminho_modelo.parent.mkdir(parents=True, exist_ok=True)
    modelo_treinado.write().overwrite().save(str(caminho_modelo))
    print(f"[SUCESSO] PipelineModel salvo em: {caminho_modelo.resolve()}")

    # Salva as métricas em JSON para facilitar a elaboração dos gráficos da Pessoa 4
    caminho_metricas.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho_metricas, "w", encoding="utf-8") as f:
        json.dump(metricas, f, indent=2, ensure_ascii=False)
    print(f"[SUCESSO] Relatório de métricas salvo em: {caminho_metricas.resolve()}")

    return modelo_treinado, metricas


def obter_ou_treinar_modelo(
    spark: SparkSession,
    caminho_modelo: Path = CAMINHO_MODELO_PADRAO,
    diretorio_raw: str = "data/raw",
) -> PipelineModel:
    """
    Carrega o `PipelineModel` já persistido em disco; caso ainda não exista,
    executa o treinamento inicial automaticamente antes de abrir o stream.
    """
    if caminho_modelo.exists() and (caminho_modelo / "metadata").exists():
        print(f"[ML] Carregando PipelineModel existente de '{caminho_modelo}'...")
        return PipelineModel.load(str(caminho_modelo))

    print(f"[ML] Modelo não encontrado em '{caminho_modelo}'. Iniciando treinamento inicial...")
    modelo, _ = treinar_e_salvar_modelo(
        spark, diretorio_raw=diretorio_raw, caminho_modelo=caminho_modelo
    )
    return modelo


if __name__ == "__main__":
    spark_session = (
        SparkSession.builder.appName("Treinamento-ML-3W-Pessoa3")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("WARN")
    try:
        treinar_e_salvar_modelo(spark_session)
    finally:
        spark_session.stop()
