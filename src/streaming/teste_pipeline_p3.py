"""
Script de Validação Autônoma do Pipeline Spark + ML (Pessoa 3).

Permite validar toda a camada de decodificação JSON (formatos Pessoa 1 e Pessoa 2),
normalização online Z-Score (parâmetros do Trabalho 2) e inferência com `pyspark.ml`
(`Imputer` + `VectorAssembler` + `StandardScaler` + `KMeans` + `RandomForestClassifier`)
simulando a recepção de pacotes Kafka sem exigir que o broker esteja ativo.
"""

import json
import time
from pyspark.sql import functions as F  # pyrefly: ignore[missing-import]
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType  # pyrefly: ignore[missing-import]

from src.streaming.consumidor_spark_ml import (
    criar_spark_session,
    aplicar_inferencia_ml_streaming,
    formatar_payload_alerta_kafka,
)
from src.streaming.preprocessamento import (
    carregar_parametros_estatisticos,
    parsear_stream_kafka,
    aplicar_preprocessamento_online,
)
from src.ml.treinar_modelo import obter_ou_treinar_modelo


def executar_teste_integrado_p3():
    """
    Executa uma bateria de testes fim a fim do Módulo 3 em memória:
      1. Simula 4 mensagens cobrindo Operação Normal, Anomalia Iminente (Transiente 106),
         Anomalia Pontual Crítica (Classe 3 - Golfada Severa) e o formato aninhado da Pessoa 2.
      2. Valida o parser unificado, a normalização Z-Score online e o modelo `pyspark.ml`.
      3. Exibe as tabelas de features, predições e payloads de alerta para o tópico `alertas-operacionais`.
    """
    print("=" * 75)
    print(" TESTE INTEGRADO AUTÔNOMO - SPARK STREAMING & PIPELINE ML (PESSOA 3)")
    print("=" * 75)

    spark = criar_spark_session(
        app_name="Teste-Integrado-Pessoa3", incluir_conector_kafka=False
    )

    try:
        parametros = carregar_parametros_estatisticos()
        modelo_ml = obter_ou_treinar_modelo(spark)

        agora = time.time()

        # Simula 4 mensagens cobrindo todos os cenários do projeto:
        # Caso 1: Formato Pessoa 1 (plano, escala Pascal do Parquet 3W) -> Operação Normal (class=0)
        # Caso 2: Formato Pessoa 1 (plano, escala Pascal, com sensor nulo) -> Anomalia Iminente (class=106)
        # Caso 3: Formato Pessoa 1 (plano, escala Pascal) -> Anomalia Pontual Crítica (class=3)
        # Caso 4: Formato Pessoa 2 (dicionário 'sensores', escala reduzida) -> Anomalia Pontual Crítica (class=8)
        mensagens_simuladas = [
            {
                "timestamp_envio": agora - 0.015,
                "arquivo_origem": "WELL-00001_20170201010207.parquet",
                "poco_id": "WELL-00001",
                "P-PDG": 26450000.0,
                "P-TPT": 14180000.0,
                "T-TPT": 76.2,
                "P-MON-CKP": 4190000.0,
                "T-JUS-CKP": 54.0,
                "P-JUS-CKGL": 11480000.0,
                "T-JUS-CKGL": 42.1,
                "QGL": 0.44,
                "class": 0,
            },
            {
                "timestamp_envio": agora - 0.012,
                "arquivo_origem": "WELL-00002_20170510040000.parquet",
                "poco_id": "WELL-00002",
                "P-PDG": 36800000.0,
                "P-TPT": 20600000.0,
                "T-TPT": 103.5,
                "P-MON-CKP": 6900000.0,
                "T-JUS-CKP": 32.0,
                "P-JUS-CKGL": None,
                "T-JUS-CKGL": 41.8,
                "QGL": 0.46,
                "class": 106,
            },
            {
                "timestamp_envio": agora - 0.010,
                "arquivo_origem": "WELL-00003_20180115091200.parquet",
                "poco_id": "WELL-00003",
                "P-PDG": 44500000.0,
                "P-TPT": 26900000.0,
                "T-TPT": 122.0,
                "P-MON-CKP": 9800000.0,
                "T-JUS-CKP": 19.5,
                "P-JUS-CKGL": 19500000.0,
                "T-JUS-CKGL": 69.0,
                "QGL": 1.65,
                "class": 3,
            },
            {
                "timestamp_evento": "2026-11-04 10:30:00",
                "timestamp_envio": agora - 0.008,
                "arquivo_origem": "WELL-00004_TESTE_P2.parquet",
                "class": 8,
                "sensores": {
                    "P-PDG": 680.0,
                    "P-TPT": 245.0,
                    "T-TPT": 120.5,
                    "P-MON-CKP": 82.0,
                },
            },
        ]

        # Empacota no mesmo formato binário/texto entregue pelo conector Kafka do Spark
        linhas_kafka = [
            (
                f"key-{idx}",
                json.dumps(msg),
                idx % 4,
                int(idx),
            )
            for idx, msg in enumerate(mensagens_simuladas)
        ]

        schema_kafka_mock = StructType(
            [
                StructField("key", StringType(), True),
                StructField("value", StringType(), True),
                StructField("partition", IntegerType(), True),
                StructField("offset", LongType(), True),
            ]
        )

        df_kafka_mock = spark.createDataFrame(linhas_kafka, schema=schema_kafka_mock).withColumn(
            "timestamp", F.current_timestamp().cast(TimestampType())
        )

        # Etapa 1: Decodificação unificada do JSON
        df_parsed = parsear_stream_kafka(df_kafka_mock)
        # Etapa 2: Normalização Z-Score online e atributos estatísticos (Trabalho 2)
        df_prep = aplicar_preprocessamento_online(df_parsed, parametros)
        # Etapa 3: Inferência com o modelo pyspark.ml (KMeans + RandomForestClassifier)
        df_pred = aplicar_inferencia_ml_streaming(df_prep, modelo_ml, parametros)

        print("\n[1] Atributos Estatísticos Online Calculados (Z-Score e Sensores Ativos):")
        df_prep.select(
            "poco_id",
            "sensores_ativos",
            F.round("P_PDG_norm", 2).alias("P_PDG_z"),
            F.round("P_TPT_norm", 2).alias("P_TPT_z"),
            F.round("T_TPT_norm", 2).alias("T_TPT_z"),
            F.round("z_score_medio", 2).alias("z_medio"),
            F.round("z_score_max", 2).alias("z_max"),
            "qtd_sensores_fora_2sigma",
            "qtd_sensores_fora_3sigma",
        ).show(truncate=False)

        print("[2] Resultado da Inferência `pyspark.ml` (Reconhecimento de Padrões e Anomalias):")
        df_pred.select(
            "poco_id",
            "status_operacional",
            "severidade",
            "cluster_padrao",
            "predicao_ml",
            "score_anomalia_ml",
            "confianca_predicao",
            "diagnostico_padrao",
            "latencia_end_to_end_ms",
        ).show(truncate=False)

        print("[3] Payload Formatado para o Tópico `alertas-operacionais` (Integração Pessoa 4):")
        df_alertas = formatar_payload_alerta_kafka(df_pred)
        df_alertas.show(truncate=False)

        print("\n[SUCESSO] Validação completa do Módulo 3 concluída!")
    finally:
        spark.stop()


if __name__ == "__main__":
    executar_teste_integrado_p3()
