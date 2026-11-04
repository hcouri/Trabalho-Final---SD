"""
Pipeline completo com o motor de alertas (Pessoa 4).

Usa as funcoes da Pessoa 3 (parser, normalizacao e ML) e, em cada micro-batch,
passa o resultado pelo motor de regras antes de publicar no Kafka.

Uso:
    python -m src.alertas.pipeline_alertas --taxa 100
"""

import argparse
import os
import time

from src.alertas.motor_regras import MotorAlertas, resumir_por_poco, criar_produtor_alertas, publicar_alertas
from src.ml.treinar_modelo import obter_ou_treinar_modelo
from src.streaming.consumidor_spark_ml import criar_spark_session, aplicar_inferencia_ml_streaming
from src.streaming.preprocessamento import (
    carregar_parametros_estatisticos,
    parsear_stream_kafka,
    aplicar_preprocessamento_online,
)


def salvar_metricas_batch(arquivo_csv, batch_id, eventos, anomalias, alertas, duracao_ms):
    novo_arquivo = not os.path.exists(arquivo_csv)
    with open(arquivo_csv, "a", encoding="utf-8") as f:
        if novo_arquivo:
            f.write("batch_id,timestamp,eventos,anomalias,alertas,duracao_ms\n")
        f.write(f"{batch_id},{time.time():.3f},{eventos},{anomalias},{alertas},{duracao_ms:.2f}\n")


def processar_batch(batch_df, batch_id, motor, producer, topico_alertas, arquivo_csv):
    inicio = time.time()

    # guarda o batch em memoria para o Spark nao recalcular o ML duas vezes
    batch_df.persist()
    total_eventos = batch_df.count()

    if total_eventos > 0:
        resumo = resumir_por_poco(batch_df)
        total_anomalias = sum(linha["pontuais"] + linha["iminentes"] for linha in resumo)

        alertas = motor.avaliar(resumo, batch_id)
        publicar_alertas(producer, topico_alertas, alertas)

        duracao_ms = (time.time() - inicio) * 1000
        print(f"[BATCH {batch_id}] eventos={total_eventos} anomalias={total_anomalias} "
              f"alertas={len(alertas)} suprimidos={motor.total_suprimidos} tempo={duracao_ms:.1f} ms")

        if arquivo_csv is not None:
            salvar_metricas_batch(arquivo_csv, batch_id, total_eventos, total_anomalias, len(alertas), duracao_ms)

    batch_df.unpersist()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topico-entrada", default="telemetria-pocos")
    parser.add_argument("--topico-alertas", default="alertas-operacionais")
    parser.add_argument("--trigger", default="2 seconds")
    parser.add_argument("--max-offsets", type=int, default=2000)
    parser.add_argument("--cooldown", type=float, default=60)
    parser.add_argument("--taxa", default="manual", help="nome do experimento (usado nos arquivos)")
    args = parser.parse_args()

    os.makedirs("resultados", exist_ok=True)
    arquivo_csv = f"resultados/batches_taxa_{args.taxa}.csv"
    # checkpoint separado por experimento, para cada um comecar do zero
    checkpoint = f"checkpoints/alertas_p4_taxa_{args.taxa}"

    spark = criar_spark_session(app_name="Pipeline-Alertas-Pessoa4")
    parametros = carregar_parametros_estatisticos()
    modelo = obter_ou_treinar_modelo(spark)

    motor = MotorAlertas(cooldown=args.cooldown)
    producer = criar_produtor_alertas(args.bootstrap_servers)

    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topico_entrada)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # mesma sequencia da Pessoa 3: parser -> normalizacao -> modelo ML
    df_parsed = parsear_stream_kafka(df_kafka)
    df_prep = aplicar_preprocessamento_online(df_parsed, parametros)
    df_pred = aplicar_inferencia_ml_streaming(df_prep, modelo, parametros)

    query = (
        df_pred.writeStream
        .trigger(processingTime=args.trigger)
        .option("checkpointLocation", checkpoint)
        .foreachBatch(lambda df, batch_id: processar_batch(
            df, batch_id, motor, producer, args.topico_alertas, arquivo_csv))
        .start()
    )

    print("Pipeline de alertas iniciado. Ctrl+C para parar.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Encerrando...")
    finally:
        query.stop()
        producer.close()
        spark.stop()
        print(f"Total de alertas: {motor.total_alertas} | Suprimidos: {motor.total_suprimidos}")


if __name__ == "__main__":
    main()