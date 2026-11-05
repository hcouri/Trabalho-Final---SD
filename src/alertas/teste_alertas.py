"""
Teste do motor de alertas sem Kafka (Pessoa 4).

Simula 3 micro-batches passando pelo pipeline da Pessoa 3 e confere as regras.

Uso:
    python -m src.alertas.teste_alertas
"""

import json
import time

from pyspark.sql import functions as F

from src.alertas.motor_regras import MotorAlertas
from src.alertas.pipeline_alertas import processar_batch
from src.ml.treinar_modelo import obter_ou_treinar_modelo
from src.streaming.consumidor_spark_ml import criar_spark_session, aplicar_inferencia_ml_streaming
from src.streaming.preprocessamento import (
    carregar_parametros_estatisticos,
    parsear_stream_kafka,
    aplicar_preprocessamento_online,
)


def gerar_evento(parametros, poco, desvio, classe):
    # cada sensor recebe: media + desvio * desvio_padrao
    evento = {"timestamp_envio": time.time(), "poco_id": poco,
              "arquivo_origem": poco + "_TESTE.parquet", "class": classe}
    for sensor, info in parametros["sensores"].items():
        evento[sensor] = info["media"] + desvio * info["desvio_padrao"]
    return evento


def criar_batch_falso(spark, eventos):
    # imita as colunas que o conector Kafka do Spark entrega
    linhas = []
    for i, evento in enumerate(eventos):
        linhas.append((evento["poco_id"], json.dumps(evento), i % 4, i))
    df = spark.createDataFrame(linhas, "key string, value string, partition int, offset long")
    return df.withColumn("timestamp", F.current_timestamp())


def main():
    spark = criar_spark_session(app_name="Teste-Alertas-P4", incluir_conector_kafka=False)
    parametros = carregar_parametros_estatisticos()
    modelo = obter_ou_treinar_modelo(spark)
    motor = MotorAlertas(cooldown=60)

    batches = [
        # batch 0: WELL-A critico (R1), WELL-B normal, WELL-C com 6 iminentes (ainda nao alerta)
        [gerar_evento(parametros, "WELL-A", 4.5, 3)]
        + [gerar_evento(parametros, "WELL-B", 0.1, 0) for _ in range(5)]
        + [gerar_evento(parametros, "WELL-C", 2.4, 106) for _ in range(6)],
        # batch 1: WELL-A critico de novo (cooldown) e WELL-C chega a 12 iminentes (R3)
        [gerar_evento(parametros, "WELL-A", 4.8, 3)]
        + [gerar_evento(parametros, "WELL-C", 2.4, 106) for _ in range(6)],
        # batch 2: WELL-C fica critico (R1 mesmo em cooldown, pois piorou)
        [gerar_evento(parametros, "WELL-C", 4.5, 6)],
    ]

    for batch_id, eventos in enumerate(batches):
        print(f"\n--- Batch {batch_id} ---")
        df = criar_batch_falso(spark, eventos)
        df_pred = aplicar_inferencia_ml_streaming(
            aplicar_preprocessamento_online(parsear_stream_kafka(df), parametros), modelo, parametros)
        processar_batch(df_pred, batch_id, motor, None, "alertas-operacionais", None)

    print(f"\nAlertas emitidos: {motor.total_alertas} (esperado 3)")
    print(f"Alertas suprimidos: {motor.total_suprimidos} (esperado 1)")
    spark.stop()


if __name__ == "__main__":
    main()