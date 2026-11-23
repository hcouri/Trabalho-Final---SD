"""
Consumidor PySpark Structured Streaming, Pipeline de Inferência ML Online
e Persistência de Saídas/Agregações em Parquet (Pessoa 3).

Este módulo cumpre integralmente as responsabilidades da Pessoa 3:
1. Assina o tópico Kafka `telemetria-pocos` via Spark Structured Streaming.
2. Aplica a camada de normalização Z-Score e pré-processamento online utilizando os
   parâmetros estatísticos validados no Trabalho 2 (`src/streaming/preprocessamento.py`).
3. Executa o `pyspark.ml.PipelineModel` (`KMeans` + `RandomForestClassifier`) em cada
   micro-batch para reconhecimento de padrões operacionais e detecção de anomalias
   pontuais ou iminentes.
4. Calcula agregações e médias em tempo real por poço (`calcular_agregacoes_por_poco`)
   e persiste as saídas validadas em **Parquet** (`resultados/parquet/`), na **Consola**
   e opcionalmente no tópico Kafka `alertas-operacionais` (integração com Pessoa 4).
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

from src.streaming.preprocessamento import (
    carregar_parametros_estatisticos,
    parsear_stream_kafka,
    aplicar_preprocessamento_online,
)
from src.ml.treinar_modelo import obter_ou_treinar_modelo, CAMINHO_MODELO_PADRAO

# Configurações padrão de conexão com o cluster Kafka (Pessoa 1, 2 e 4)
KAFKA_BROKER_PADRAO = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICO_ENTRADA_PADRAO = os.getenv("KAFKA_TOPIC_TELEMETRIA", "telemetria-pocos")
TOPICO_ALERTAS_PADRAO = os.getenv("KAFKA_TOPIC_ALERTAS", "alertas-operacionais")
CHECKPOINT_DIR_PADRAO = "checkpoints/spark_streaming_3w"
DIR_PARQUET_PADRAO = "resultados/parquet"


def criar_spark_session(
    app_name: str = "SparkStreaming-3W-ML-Pessoa3",
    incluir_conector_kafka: bool = True,
) -> SparkSession:
    """
    Inicializa a SparkSession otimizada para baixa latência em Structured Streaming,
    com configurações seguras para execução no Windows/Java 17+ e carregamento
    automático do conector oficial `spark-sql-kafka-0-10`.
    """
    # Garante compatibilidade local em ambientes Windows (mesmo padrão validado pela Pessoa 2)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    versao_spark = pyspark.__version__.split(".dev")[0]
    # Spark 3.x utiliza Scala 2.12; Spark 4.x utiliza Scala 2.13
    scala_binary = "2.13" if versao_spark.startswith("4.") else "2.12"
    pacote_kafka = f"org.apache.spark:spark-sql-kafka-0-10_{scala_binary}:{versao_spark}"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.host", os.getenv("SPARK_LOCAL_IP", "127.0.0.1"))
        .config("spark.driver.bindAddress", os.getenv("SPARK_LOCAL_IP", "127.0.0.1"))
        # 4 partições de shuffle alinham-se às 4 partições do tópico Kafka (definidas pela Pessoa 2)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )

    if incluir_conector_kafka:
        builder = builder.config("spark.jars.packages", pacote_kafka)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def aplicar_inferencia_ml_streaming(
    df_preprocessado: DataFrame,
    modelo_ml: PipelineModel,
    parametros: Optional[Dict[str, Any]] = None,
) -> DataFrame:
    """
    Integra o `pyspark.ml.PipelineModel` ao DataFrame de streaming pré-processado,
    extraindo probabilidades por classe, cluster de padrão operacional (`cluster_padrao`),
    classificação de anomalia (`NORMAL`, `ANOMALIA_IMINENTE`, `ANOMALIA_PONTUAL`),
    severidade e latência end-to-end em milissegundos.
    """
    if parametros is None:
        parametros = carregar_parametros_estatisticos()

    mapa_classes = parametros.get("mapa_classes_3w", {})

    # Aplica os 5 estágios do PipelineModel: Imputer -> VectorAssembler -> StandardScaler -> KMeans -> RandomForest
    df_pred = modelo_ml.transform(df_preprocessado)

    # Converte o vetor de probabilidades do RandomForest em array para leitura direta das colunas
    df_pred = df_pred.withColumn("prob_array", vector_to_array(F.col("probabilidade_ml")))

    df_pred = (
        df_pred.withColumn("prob_normal", F.round(F.col("prob_array").getItem(0), 4))
        .withColumn("prob_anomalia_iminente", F.round(F.col("prob_array").getItem(1), 4))
        .withColumn("prob_anomalia_pontual", F.round(F.col("prob_array").getItem(2), 4))
        .withColumn("score_anomalia_ml", F.round(F.lit(1.0) - F.col("prob_normal"), 4))
        .withColumn(
            "confianca_predicao",
            F.round(
                F.greatest(
                    F.col("prob_normal"),
                    F.col("prob_anomalia_iminente"),
                    F.col("prob_anomalia_pontual"),
                ),
                4,
            ),
        )
    )

    # Classificação híbrida (combina predição do RandomForest com limites estatísticos 2σ e 3σ do Trabalho 2)
    df_pred = df_pred.withColumn(
        "status_operacional",
        F.when(
            (F.col("predicao_ml") == 2.0)
            | (F.col("qtd_sensores_fora_3sigma") >= 1.0),
            F.lit("ANOMALIA_PONTUAL"),
        )
        .when(
            (F.col("predicao_ml") == 1.0)
            | (F.col("qtd_sensores_fora_2sigma") >= 2.0)
            | (F.col("score_anomalia_ml") >= 0.45),
            F.lit("ANOMALIA_IMINENTE"),
        )
        .otherwise(F.lit("NORMAL")),
    ).withColumn(
        "severidade",
        F.when(F.col("status_operacional") == "ANOMALIA_PONTUAL", F.lit("CRITICA"))
        .when(F.col("status_operacional") == "ANOMALIA_IMINENTE", F.lit("ALERTA_PREVENTIVO"))
        .otherwise(F.lit("NORMAL")),
    )

    # Mapeia o código de falha 3W (ou diagnóstico inferido pelo cluster + classificador)
    itens_mapa = [F.lit(str(x)) for par in mapa_classes.items() for x in par]
    expr_mapa = F.create_map(*itens_mapa) if itens_mapa else F.create_map()
    df_pred = df_pred.withColumn(
        "diagnostico_padrao",
        F.when(
            (F.col("classe_real") != 0)
            & expr_mapa.getItem(F.col("classe_real").cast("string")).isNotNull(),
            expr_mapa.getItem(F.col("classe_real").cast("string")),
        )
        .when(
            F.col("status_operacional") == "ANOMALIA_PONTUAL",
            F.concat(
                F.lit("Anomalia Pontual Detectada pelo ML (Cluster "),
                F.col("cluster_padrao").cast("string"),
                F.lit(")"),
            ),
        )
        .when(
            F.col("status_operacional") == "ANOMALIA_IMINENTE",
            F.concat(
                F.lit("Tendencia Transiente / Anomalia Iminente (Cluster "),
                F.col("cluster_padrao").cast("string"),
                F.lit(")"),
            ),
        )
        .otherwise(F.lit("Operacao Normal")),
    )

    # Medição de latência end-to-end: diferença entre o término da inferência no Spark e o timestamp_envio do Produtor
    ts_atual_sec = F.current_timestamp().cast("double")
    df_pred = df_pred.withColumn("timestamp_inferencia", ts_atual_sec).withColumn(
        "latencia_end_to_end_ms",
        F.when(
            F.col("timestamp_envio").isNotNull(),
            F.round((F.col("timestamp_inferencia") - F.col("timestamp_envio")) * 1000.0, 2),
        ).otherwise(F.lit(0.0)),
    )

    return df_pred


def calcular_agregacoes_por_poco(df_predicoes: DataFrame, batch_id: int = 0) -> DataFrame:
    """
    Calcula agregações e médias operacionais por poço (`poco_id`) em cada micro-batch:
      - Total de eventos processados, anomalias pontuais e anomalias iminentes
      - Médias dos sensores principais (`P-PDG`, `P-TPT`, `T-TPT`, `P-MON-CKP`)
      - Score ML médio, Z-Score máximo, latência média end-to-end e taxa de anomalia (%)
    Essas agregações são exibidas na consola e persistidas em Parquet.
    """
    pontual = F.col("status_operacional") == "ANOMALIA_PONTUAL"
    iminente = F.col("status_operacional") == "ANOMALIA_IMINENTE"
    anomalia = F.col("status_operacional") != "NORMAL"

    return (
        df_predicoes.groupBy("poco_id")
        .agg(
            F.lit(batch_id).alias("batch_id"),
            F.current_timestamp().alias("timestamp_agregacao"),
            F.count("*").alias("total_eventos"),
            F.sum(F.when(anomalia, 1).otherwise(0)).alias("total_anomalias"),
            F.sum(F.when(pontual, 1).otherwise(0)).alias("anomalias_pontuais"),
            F.sum(F.when(iminente, 1).otherwise(0)).alias("anomalias_iminentes"),
            F.round(F.avg(F.col("P_PDG_imp")), 2).alias("media_p_pdg"),
            F.round(F.avg(F.col("P_TPT_imp")), 2).alias("media_p_tpt"),
            F.round(F.avg(F.col("T_TPT_imp")), 2).alias("media_t_tpt"),
            F.round(F.avg(F.col("P_MON_CKP_imp")), 2).alias("media_p_mon_ckp"),
            F.round(F.avg("z_score_medio"), 3).alias("z_score_medio_poco"),
            F.round(F.max("z_score_max"), 3).alias("z_score_max_poco"),
            F.round(F.avg("score_anomalia_ml"), 4).alias("score_ml_medio"),
            F.round(F.avg("latencia_end_to_end_ms"), 2).alias("latencia_media_ms"),
        )
        .withColumn(
            "taxa_anomalia_pct",
            F.round((F.col("total_anomalias") / F.col("total_eventos")) * 100.0, 2),
        )
        .orderBy(F.desc("total_anomalias"), F.asc("poco_id"))
    )


def extrair_alertas_estruturados(df_predicoes: DataFrame, batch_id: int = 0) -> DataFrame:
    """
    Seleciona os registros anômalos (`ANOMALIA_IMINENTE` ou `ANOMALIA_PONTUAL`)
    com colunas planas e tipadas para persistência analítica em Parquet.
    """
    return (
        df_predicoes.filter(F.col("status_operacional") != "NORMAL")
        .select(
            F.lit(batch_id).alias("batch_id"),
            F.col("poco_id"),
            F.col("arquivo_origem"),
            F.col("timestamp_evento_str").alias("timestamp_evento"),
            F.col("timestamp_envio"),
            F.col("timestamp_inferencia"),
            F.col("latencia_end_to_end_ms"),
            F.col("status_operacional"),
            F.col("severidade"),
            F.col("cluster_padrao"),
            F.col("predicao_ml"),
            F.col("score_anomalia_ml"),
            F.col("confianca_predicao"),
            F.round(F.col("z_score_medio"), 3).alias("z_score_medio"),
            F.round(F.col("z_score_max"), 3).alias("z_score_max"),
            F.col("qtd_sensores_fora_2sigma").cast("int").alias("sensores_acima_2sigma"),
            F.col("qtd_sensores_fora_3sigma").cast("int").alias("sensores_acima_3sigma"),
            F.col("classe_real"),
            F.col("diagnostico_padrao"),
        )
    )


def persistir_saidas_parquet(
    batch_df: DataFrame,
    df_agregacoes: DataFrame,
    df_alertas_estruturados: DataFrame,
    batch_id: int,
    diretorio_parquet: str = DIR_PARQUET_PADRAO,
) -> Dict[str, Any]:
    """
    Persiste as três camadas de saída do pipeline em formato colunar **Parquet**
    (modo append), garantindo rastreabilidade e auditoria pós-execução:
      1. `<diretorio_parquet>/eventos_processados` (particionado por `poco_id`)
      2. `<diretorio_parquet>/agregacoes_pocos` (médias e contagens por poço/batch)
      3. `<diretorio_parquet>/alertas_anomalias` (somente anomalias detectadas)
    """
    base = Path(diretorio_parquet)
    path_eventos = str(base / "eventos_processados")
    path_agregacoes = str(base / "agregacoes_pocos")
    path_alertas = str(base / "alertas_anomalias")

    # 1. Camada Silver: eventos normalizados + resultado da inferência ML (sem colunas Vector complexas)
    df_eventos_saida = batch_df.select(
        F.lit(batch_id).alias("batch_id"),
        "poco_id",
        "arquivo_origem",
        "timestamp_evento_str",
        "timestamp_envio",
        "timestamp_inferencia",
        "latencia_end_to_end_ms",
        "classe_real",
        "sensores_ativos",
        "P_PDG_imp",
        "P_TPT_imp",
        "T_TPT_imp",
        "P_MON_CKP_imp",
        "z_score_medio",
        "z_score_max",
        "qtd_sensores_fora_2sigma",
        "qtd_sensores_fora_3sigma",
        "cluster_padrao",
        "predicao_ml",
        "score_anomalia_ml",
        "confianca_predicao",
        "status_operacional",
        "severidade",
        "diagnostico_padrao",
    )

    df_eventos_saida.write.mode("append").partitionBy("poco_id").parquet(path_eventos)

    # 2. Camada Gold (Agregações/Médias por Poço)
    df_agregacoes.write.mode("append").parquet(path_agregacoes)

    # 3. Camada Gold (Alertas de Anomalias)
    qtd_alertas = df_alertas_estruturados.count()
    if qtd_alertas > 0:
        df_alertas_estruturados.write.mode("append").parquet(path_alertas)

    return {
        "eventos_path": path_eventos,
        "agregacoes_path": path_agregacoes,
        "alertas_path": path_alertas,
        "alertas_gravados": qtd_alertas,
    }


def formatar_payload_alerta_kafka(df_predicoes: DataFrame) -> DataFrame:
    """
    Filtra apenas eventos anômalos (`ANOMALIA_IMINENTE` ou `ANOMALIA_PONTUAL`) e
    serializa em estrutura chave/valor JSON pronta para envio ao tópico
    `alertas-operacionais` (ponto de integração direta com a Pessoa 4).
    """
    df_anomalias = df_predicoes.filter(F.col("status_operacional") != "NORMAL")

    colunas_alerta = F.struct(
        F.col("poco_id"),
        F.col("arquivo_origem"),
        F.col("timestamp_evento_str").alias("timestamp_evento"),
        F.col("timestamp_envio"),
        F.col("timestamp_inferencia"),
        F.col("latencia_end_to_end_ms"),
        F.col("status_operacional"),
        F.col("severidade"),
        F.col("cluster_padrao"),
        F.col("predicao_ml"),
        F.col("score_anomalia_ml"),
        F.col("confianca_predicao"),
        F.round(F.col("z_score_medio"), 3).alias("z_score_medio"),
        F.round(F.col("z_score_max"), 3).alias("z_score_max"),
        F.col("qtd_sensores_fora_2sigma").cast("int").alias("sensores_acima_2sigma"),
        F.col("qtd_sensores_fora_3sigma").cast("int").alias("sensores_acima_3sigma"),
        F.col("classe_real"),
        F.col("diagnostico_padrao"),
    )

    return df_anomalias.select(
        F.col("poco_id").cast("string").alias("key"),
        F.to_json(colunas_alerta).alias("value"),
    )


def _processar_micro_batch(
    batch_df: DataFrame,
    batch_id: int,
    publicar_alertas: bool = False,
    salvar_parquet: bool = True,
    diretorio_parquet: str = DIR_PARQUET_PADRAO,
    bootstrap_servers: str = KAFKA_BROKER_PADRAO,
    topico_alertas: str = TOPICO_ALERTAS_PADRAO,
):
    """
    Função callback invocada pelo `foreachBatch` a cada micro-batch do Spark Streaming.
    Exibe resumo executivo e agregações por poço na consola, persiste as saídas
    em Parquet (`eventos_processados`, `agregacoes_pocos`, `alertas_anomalias`)
    e publica alertas no tópico Kafka se configurado.
    """
    total_eventos = batch_df.count()
    if total_eventos == 0:
        return

    inicio_batch = time.time()
    batch_df.cache()

    try:
        resumo = (
            batch_df.groupBy("status_operacional")
            .agg(
                F.count("*").alias("qtd"),
                F.round(F.avg("score_anomalia_ml"), 4).alias("score_medio"),
                F.round(F.avg("z_score_max"), 2).alias("z_max_medio"),
                F.round(F.avg("latencia_end_to_end_ms"), 2).alias("latencia_media_ms"),
            )
            .orderBy("status_operacional")
            .collect()
        )

        # Calcula agregações/médias por poço e extrai alertas estruturados do micro-batch
        df_agregacoes = calcular_agregacoes_por_poco(batch_df, batch_id=batch_id)
        df_alertas_estruturados = extrair_alertas_estruturados(batch_df, batch_id=batch_id)

        print(f"\n{'=' * 90}")
        print(
            f" [SPARK STREAMING + ML | BATCH #{batch_id}] "
            f"Eventos processados: {total_eventos}"
        )
        print(f"{'-' * 90}")
        for r in resumo:
            print(
                f"  -> Status: {r['status_operacional']:<18} | "
                f"Qtd: {r['qtd']:<5} | "
                f"Score ML Médio: {r['score_medio']:<6} | "
                f"Z-Max Médio: {r['z_max_medio']:<5} | "
                f"Latência E2E: {r['latencia_media_ms']} ms"
            )
        print(f"{'-' * 90}")

        print(" [AGREGAÇÕES E MÉDIAS POR POÇO - CONSOLA]")
        df_agregacoes.select(
            "poco_id",
            "total_eventos",
            "total_anomalias",
            "anomalias_pontuais",
            "anomalias_iminentes",
            "taxa_anomalia_pct",
            "z_score_max_poco",
            "score_ml_medio",
            "latencia_media_ms",
        ).show(10, truncate=False)

        colunas_exibicao = [
            "poco_id",
            "status_operacional",
            "severidade",
            "cluster_padrao",
            "score_anomalia_ml",
            "z_score_max",
            "classe_real",
            "diagnostico_padrao",
            "latencia_end_to_end_ms",
        ]
        batch_df.select(*colunas_exibicao).show(10, truncate=False)

        if salvar_parquet:
            info_parquet = persistir_saidas_parquet(
                batch_df=batch_df,
                df_agregacoes=df_agregacoes,
                df_alertas_estruturados=df_alertas_estruturados,
                batch_id=batch_id,
                diretorio_parquet=diretorio_parquet,
            )
            print(
                f" [PARQUET PERSISTIDO] Diretório: '{diretorio_parquet}' | "
                f"Eventos: {total_eventos} | Alertas gravados: {info_parquet['alertas_gravados']}"
            )

        if publicar_alertas:
            df_alertas = formatar_payload_alerta_kafka(batch_df)
            qtd_alertas = df_alertas.count()
            if qtd_alertas > 0:
                (
                    df_alertas.write.format("kafka")
                    .option("kafka.bootstrap.servers", bootstrap_servers)
                    .option("topic", topico_alertas)
                    .save()
                )
                print(
                    f" [ALERTA KAFKA] {qtd_alertas} anomalias publicadas no tópico '{topico_alertas}'."
                )

        duracao_ms = (time.time() - inicio_batch) * 1000.0
        print(f" Tempo de processamento do micro-batch #{batch_id}: {duracao_ms:.2f} ms")
        print(f"{'=' * 90}")
    finally:
        batch_df.unpersist()


def iniciar_streaming(
    bootstrap_servers: str = KAFKA_BROKER_PADRAO,
    topico_entrada: str = TOPICO_ENTRADA_PADRAO,
    topico_alertas: str = TOPICO_ALERTAS_PADRAO,
    starting_offsets: str = "latest",
    intervalo_trigger: str = "2 seconds",
    publicar_alertas: bool = False,
    salvar_parquet: bool = True,
    diretorio_parquet: str = DIR_PARQUET_PADRAO,
    max_offsets_por_trigger: int = 2000,
):
    """
    Inicia o pipeline completo de Structured Streaming assinando o tópico Kafka,
    aplicando a normalização online do Trabalho 2, executando a inferência `pyspark.ml`
    e gravando as saídas validadas na consola, em Parquet e/ou no Kafka.
    """
    spark = criar_spark_session(incluir_conector_kafka=True)
    parametros = carregar_parametros_estatisticos()
    modelo_ml = obter_ou_treinar_modelo(spark, caminho_modelo=CAMINHO_MODELO_PADRAO)

    print("=" * 78)
    print(" INICIANDO CONSUMIDOR SPARK STRUCTURED STREAMING + PIPELINE ML (PESSOA 3)")
    print("=" * 78)
    print(f" Broker Kafka           : {bootstrap_servers}")
    print(f" Tópico Assinado        : {topico_entrada} (startingOffsets={starting_offsets})")
    print(f" Modelo Carregado       : {CAMINHO_MODELO_PADRAO}")
    print(f" Intervalo de MicroBatch: {intervalo_trigger}")
    print(f" Persistência Parquet   : {'ATIVO (' + diretorio_parquet + ')' if salvar_parquet else 'DESATIVADO'}")
    print(f" Publicar Alertas Kafka : {'ATIVO (' + topico_alertas + ')' if publicar_alertas else 'DESATIVADO'}")
    print("=" * 78)

    # 1. Assinatura do tópico Kafka em tempo real
    df_kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topico_entrada)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", max_offsets_por_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Decodificação JSON e unificação de esquema (compatível com Pessoa 1 e Pessoa 2)
    df_parsed = parsear_stream_kafka(df_kafka_raw)

    # 3. Normalização Z-Score online e atributos estatísticos (parâmetros do Trabalho 2)
    df_preprocessado = aplicar_preprocessamento_online(df_parsed, parametros)

    # 4. Reconhecimento de padrões (KMeans) e detecção de anomalias (RandomForest) com pyspark.ml
    df_predicoes = aplicar_inferencia_ml_streaming(df_preprocessado, modelo_ml, parametros)

    # 5. Execução contínua em micro-batches (Consola + Parquet + Kafka opcional)
    query = (
        df_predicoes.writeStream.outputMode("append")
        .trigger(processingTime=intervalo_trigger)
        .option("checkpointLocation", CHECKPOINT_DIR_PADRAO)
        .foreachBatch(
            lambda df, b_id: _processar_micro_batch(
                df,
                b_id,
                publicar_alertas=publicar_alertas,
                salvar_parquet=salvar_parquet,
                diretorio_parquet=diretorio_parquet,
                bootstrap_servers=bootstrap_servers,
                topico_alertas=topico_alertas,
            )
        )
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[INFO] Encerrando Spark Structured Streaming...")
    finally:
        query.stop()
        spark.stop()


def parse_args():
    """Configura os argumentos de linha de comando para facilitar experimentos e benchmarks."""
    parser = argparse.ArgumentParser(
        description="Consumidor PySpark Structured Streaming + Pipeline ML + Persistência Parquet (Pessoa 3)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=KAFKA_BROKER_PADRAO,
        help="Endereço do broker Kafka (padrão: localhost:9092)",
    )
    parser.add_argument(
        "--topico-entrada",
        default=TOPICO_ENTRADA_PADRAO,
        help="Tópico Kafka de telemetria a assinar (padrão: telemetria-pocos)",
    )
    parser.add_argument(
        "--topico-alertas",
        default=TOPICO_ALERTAS_PADRAO,
        help="Tópico Kafka para envio de anomalias (padrão: alertas-operacionais)",
    )
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["earliest", "latest"],
        help="Offset inicial de leitura do Kafka (earliest ou latest)",
    )
    parser.add_argument(
        "--trigger",
        default="2 seconds",
        help="Intervalo de micro-batch do Structured Streaming (ex: '2 seconds')",
    )
    parser.add_argument(
        "--dir-parquet",
        default=DIR_PARQUET_PADRAO,
        help="Diretório de saída dos arquivos Parquet (padrão: resultados/parquet)",
    )
    parser.add_argument(
        "--sem-parquet",
        action="store_true",
        help="Desativa a gravação em Parquet (mantém apenas consola/Kafka)",
    )
    parser.add_argument(
        "--publicar-alertas",
        action="store_true",
        help="Se ativado, publica anomalias detectadas no tópico alertas-operacionais",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    iniciar_streaming(
        bootstrap_servers=args.bootstrap_servers,
        topico_entrada=args.topico_entrada,
        topico_alertas=args.topico_alertas,
        starting_offsets=args.starting_offsets,
        intervalo_trigger=args.trigger,
        publicar_alertas=args.publicar_alertas,
        salvar_parquet=not args.sem_parquet,
        diretorio_parquet=args.dir_parquet,
    )
