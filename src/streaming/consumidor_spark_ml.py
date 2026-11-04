"""
Consumidor PySpark Structured Streaming e Pipeline de Inferência ML Online (Pessoa 3).

Este módulo cumpre as Tarefas 1, 2 e 3 da Pessoa 3 em tempo real:
1. Assina o tópico Kafka `telemetria-pocos` via Spark Structured Streaming.
2. Aplica a camada de normalização Z-Score e pré-processamento online utilizando os
   parâmetros estatísticos validados no Trabalho 2 (`src/streaming/preprocessamento.py`).
3. Executa o `pyspark.ml.PipelineModel` (`KMeans` + `RandomForestClassifier`) em cada
   micro-batch para reconhecimento de padrões operacionais e detecção de anomalias
   pontuais ou iminentes.
4. Calcula a latência end-to-end (`latencia_end_to_end_ms`) desde o `timestamp_envio`
   do Produtor Kafka até a conclusão da inferência no Spark e permite publicar
   automaticamente anomalias no tópico `alertas-operacionais` (integração com Pessoa 4).
"""

import argparse
import os
import time
from typing import Dict, Any, Optional

import pyspark  # pyrefly: ignore[missing-import]
from pyspark.sql import SparkSession, DataFrame  # pyrefly: ignore[missing-import]
from pyspark.sql import functions as F  # pyrefly: ignore[missing-import]
from pyspark.ml import PipelineModel  # pyrefly: ignore[missing-import]
from pyspark.ml.functions import vector_to_array  # pyrefly: ignore[missing-import]

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


def criar_spark_session(
    app_name: str = "SparkStreaming-3W-ML-Pessoa3",
    incluir_conector_kafka: bool = True,
) -> SparkSession:
    """
    Inicializa a SparkSession otimizada para baixa latência em Structured Streaming
    e carrega automaticamente o pacote oficial `spark-sql-kafka-0-10` compatível
    com a versão do PySpark instalada.
    """
    versao_spark = pyspark.__version__.split(".dev")[0]
    # Spark 3.x utiliza Scala 2.12; Spark 4.x utiliza Scala 2.13
    scala_binary = "2.13" if versao_spark.startswith("4.") else "2.12"
    pacote_kafka = f"org.apache.spark:spark-sql-kafka-0-10_{scala_binary}:{versao_spark}"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
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
    expr_mapa = F.create_map([F.lit(x) for par in mapa_classes.items() for x in par])
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
    ts_atual_sec = (
        F.unix_timestamp(F.current_timestamp()).cast("double")
        + (F.unix_millis(F.current_timestamp()) % 1000) / 1000.0
    )
    df_pred = df_pred.withColumn("timestamp_inferencia", ts_atual_sec).withColumn(
        "latencia_end_to_end_ms",
        F.when(
            F.col("timestamp_envio").isNotNull(),
            F.round((F.col("timestamp_inferencia") - F.col("timestamp_envio")) * 1000.0, 2),
        ).otherwise(F.lit(0.0)),
    )

    return df_pred


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
    bootstrap_servers: str = KAFKA_BROKER_PADRAO,
    topico_alertas: str = TOPICO_ALERTAS_PADRAO,
):
    """
    Função callback invocada pelo `foreachBatch` a cada micro-batch do Spark Streaming.
    Exibe resumo executivo, amostra de predições e publica alertas no Kafka se configurado.
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

        print(f"\n{'=' * 85}")
        print(
            f" [SPARK STREAMING + ML | BATCH #{batch_id}] "
            f"Eventos processados: {total_eventos}"
        )
        print(f"{'-' * 85}")
        for r in resumo:
            print(
                f"  -> Status: {r['status_operacional']:<18} | "
                f"Qtd: {r['qtd']:<5} | "
                f"Score ML Médio: {r['score_medio']:<6} | "
                f"Z-Max Médio: {r['z_max_medio']:<5} | "
                f"Latência E2E: {r['latencia_media_ms']} ms"
            )
        print(f"{'-' * 85}")

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
        print(f"{'=' * 85}")
    finally:
        batch_df.unpersist()


def iniciar_streaming(
    bootstrap_servers: str = KAFKA_BROKER_PADRAO,
    topico_entrada: str = TOPICO_ENTRADA_PADRAO,
    topico_alertas: str = TOPICO_ALERTAS_PADRAO,
    starting_offsets: str = "latest",
    intervalo_trigger: str = "2 seconds",
    publicar_alertas: bool = False,
    max_offsets_por_trigger: int = 2000,
):
    """
    Inicia o pipeline completo de Structured Streaming assinando o tópico Kafka,
    aplicando a normalização online do Trabalho 2 e executando a inferência `pyspark.ml`.
    """
    spark = criar_spark_session(incluir_conector_kafka=True)
    parametros = carregar_parametros_estatisticos()
    modelo_ml = obter_ou_treinar_modelo(spark, caminho_modelo=CAMINHO_MODELO_PADRAO)

    print("=" * 75)
    print(" INICIANDO CONSUMIDOR SPARK STRUCTURED STREAMING + PIPELINE ML (PESSOA 3)")
    print("=" * 75)
    print(f" Broker Kafka           : {bootstrap_servers}")
    print(f" Tópico Assinado        : {topico_entrada} (startingOffsets={starting_offsets})")
    print(f" Modelo Carregado       : {CAMINHO_MODELO_PADRAO}")
    print(f" Intervalo de MicroBatch: {intervalo_trigger}")
    print(f" Publicar Alertas Kafka : {'ATIVO (' + topico_alertas + ')' if publicar_alertas else 'DESATIVADO'}")
    print("=" * 75)

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

    # 5. Execução contínua em micro-batches
    query = (
        df_predicoes.writeStream.outputMode("append")
        .trigger(processingTime=intervalo_trigger)
        .option("checkpointLocation", CHECKPOINT_DIR_PADRAO)
        .foreachBatch(
            lambda df, b_id: _processar_micro_batch(
                df,
                b_id,
                publicar_alertas=publicar_alertas,
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
        description="Consumidor PySpark Structured Streaming + Pipeline ML (Pessoa 3)"
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
    )
