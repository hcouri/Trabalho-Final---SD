"""
Script de Validação Autônoma do Pipeline Spark + ML + Persistência Parquet (Pessoa 3).

Permite validar de ponta a ponta sem exigir o broker Kafka ativo:
1. Decodificação JSON unificada (Pessoa 1 legado, Pessoa 1 multi-poço com `evento_classe` e Pessoa 2 `sensores`).
2. Normalização online Z-Score com os parâmetros estatísticos do Trabalho 2.
3. Inferência `pyspark.ml` (`Imputer` + `VectorAssembler` + `StandardScaler` + `KMeans` + `RandomForestClassifier`).
4. Cálculo de agregações/médias por poço (`calcular_agregacoes_por_poco`).
5. Gravação e releitura de verificação em **Parquet** (`resultados/parquet_teste/`).
"""

import json
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

from src.streaming.consumidor_spark_ml import (
    criar_spark_session,
    aplicar_inferencia_ml_streaming,
    calcular_agregacoes_por_poco,
    extrair_alertas_estruturados,
    persistir_saidas_parquet,
    formatar_payload_alerta_kafka,
)
from src.streaming.preprocessamento import (
    carregar_parametros_estatisticos,
    parsear_stream_kafka,
    aplicar_preprocessamento_online,
)
from src.ml.treinar_modelo import obter_ou_treinar_modelo

DIR_PARQUET_TESTE = Path("resultados/parquet_teste_p3")


def executar_teste_integrado_p3():
    """
    Executa a validação integrada do Módulo 3 em memória e em disco (Parquet):
      1. Simula 4 mensagens cobrindo Operação Normal, Anomalia Iminente (Transiente 106),
         Anomalia Pontual Crítica (Classe 3 - Golfada Severa) com `evento_classe` e o formato da Pessoa 2.
      2. Valida o parser unificado, a normalização Z-Score online e o modelo `pyspark.ml`.
      3. Calcula agregações/médias por poço, persiste em Parquet e relê os arquivos para confirmar integridade.
    """
    print("=" * 80)
    print(" TESTE INTEGRADO AUTÔNOMO - SPARK STREAMING, ML & PARQUET (PESSOA 3)")
    print("=" * 80)

    spark = criar_spark_session(
        app_name="Teste-Integrado-Pessoa3", incluir_conector_kafka=False
    )

    try:
        parametros = carregar_parametros_estatisticos()
        modelo_ml = obter_ou_treinar_modelo(spark)

        agora = time.time()

        # Simula 4 mensagens cobrindo todos os cenários do projeto:
        # Caso 1: Formato Pessoa 1 (plano, escala Pascal do Parquet 3W) -> Operação Normal (class=0)
        # Caso 2: Formato Pessoa 1 (com sensor nulo + evento_classe) -> Anomalia Iminente (class=106)
        # Caso 3: Formato Pessoa 1 multi-poço 1Hz (com evento_classe="3") -> Anomalia Pontual Crítica
        # Caso 4: Formato Pessoa 2 (dicionário 'sensores', escala reduzida) -> Anomalia Pontual Crítica
        mensagens_simuladas: List[Dict[str, Any]] = [
            {
                "timestamp_envio": agora - 0.015,
                "arquivo_origem": "WELL-00001_20170201010207.parquet",
                "poco_id": "WELL-00001",
                "evento_classe": "0",
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
                "evento_classe": "106",
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
                "evento_classe": "3",
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
                "timestamp_evento": "2026-11-23 10:30:00",
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

        linhas_kafka = [
            (
                f"key-{idx}",
                json.dumps(msg),
                idx % 4,
                idx,
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

        # Etapas 1, 2 e 3: Parser -> Normalização Online (Trabalho 2) -> Inferência ML
        df_parsed = parsear_stream_kafka(df_kafka_mock)
        df_prep = aplicar_preprocessamento_online(df_parsed, parametros)
        df_pred = aplicar_inferencia_ml_streaming(df_prep, modelo_ml, parametros)

        # Etapa 4: Agregações/Médias por poço e extração de alertas estruturados
        df_agregacoes = calcular_agregacoes_por_poco(df_pred, batch_id=0)
        df_alertas_est = extrair_alertas_estruturados(df_pred, batch_id=0)

        print("\n[1] Atributos Estatísticos Online Calculados (Z-Score e Sensores Ativos):")
        df_prep.select(
            "poco_id",
            "evento_classe",
            "sensores_ativos",
            F.round("P_PDG_norm", 2).alias("P_PDG_z"),
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
            "diagnostico_padrao",
            "latencia_end_to_end_ms",
        ).show(truncate=False)

        print("[3] Agregações e Médias por Poço (Consola + Parquet):")
        df_agregacoes.select(
            "poco_id",
            "total_eventos",
            "total_anomalias",
            "taxa_anomalia_pct",
            "media_p_pdg",
            "z_score_max_poco",
            "score_ml_medio",
            "latencia_media_ms",
        ).show(truncate=False)

        # Etapa 5: Validação de persistência real em Parquet + releitura de auditoria
        if DIR_PARQUET_TESTE.exists():
            shutil.rmtree(DIR_PARQUET_TESTE)

        info_parquet = persistir_saidas_parquet(
            batch_df=df_pred,
            df_agregacoes=df_agregacoes,
            df_alertas_estruturados=df_alertas_est,
            batch_id=0,
            diretorio_parquet=str(DIR_PARQUET_TESTE),
        )

        qtd_ev_lidos = spark.read.parquet(info_parquet["eventos_path"]).count()
        qtd_ag_lidos = spark.read.parquet(info_parquet["agregacoes_path"]).count()
        qtd_al_lidos = spark.read.parquet(info_parquet["alertas_path"]).count()
        print(
            f"[4] Auditoria Parquet ({DIR_PARQUET_TESTE}): "
            f"eventos={qtd_ev_lidos} (esperado 4) | "
            f"agregacoes={qtd_ag_lidos} (esperado 4) | "
            f"alertas={qtd_al_lidos} (esperado 3)"
        )
        assert qtd_ev_lidos == 4, f"Esperado 4 eventos gravados, lido {qtd_ev_lidos}"
        assert qtd_ag_lidos == 4, f"Esperado 4 agregações gravadas, lido {qtd_ag_lidos}"
        assert qtd_al_lidos == 3, f"Esperado 3 alertas gravados, lido {qtd_al_lidos}"

        print("\n[5] Payload Formatado para o Tópico `alertas-operacionais` (Integração Pessoa 4):")
        df_alertas_kafka = formatar_payload_alerta_kafka(df_pred)
        df_alertas_kafka.show(truncate=False)

        print("\n[SUCESSO] Validação completa (Streaming + ML + Parquet + Alertas) concluída!")
    finally:
        spark.stop()
        if DIR_PARQUET_TESTE.exists():
            shutil.rmtree(DIR_PARQUET_TESTE, ignore_errors=True)


if __name__ == "__main__":
    executar_teste_integrado_p3()
