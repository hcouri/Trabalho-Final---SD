"""
Mede a latencia end-to-end dos alertas (Pessoa 4).

Consome o topico alertas-operacionais e calcula, para cada alerta:
  latencia = momento em que o alerta chegou aqui - timestamp_envio do evento no produtor

Uso:
    python -m src.benchmarks.medir_latencia --taxa 100 --duracao 150
"""

import argparse
import json
import os
import time

import numpy as np
from kafka import KafkaConsumer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topico", default="alertas-operacionais")
    parser.add_argument("--taxa", default="manual", help="nome do experimento (usado no arquivo)")
    parser.add_argument("--duracao", type=int, default=150, help="tempo de coleta em segundos")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topico,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="latest",
        group_id=f"benchmark-latencia-{args.taxa}-{int(time.time())}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    os.makedirs("resultados", exist_ok=True)
    arquivo = f"resultados/latencia_taxa_{args.taxa}.csv"
    f = open(arquivo, "w", encoding="utf-8")
    f.write("poco_id,regra,latencia_primeiro_ms,latencia_ultimo_ms,latencia_disparo_kafka_ms\n")

    latencias = []
    fim = time.time() + args.duracao
    print(f"Coletando alertas por {args.duracao} s...")

    while time.time() < fim:
        try:
            mensagens = consumer.poll(timeout_ms=1000)
        except ValueError:
            # falha da biblioteca kafka no Windows quando uma conexao cai; tenta de novo
            print("Conexao com o Kafka caiu, reconectando...")
            continue
        for lista in mensagens.values():
            for msg in lista:
                recebido = time.time()
                alerta = msg.value

                # do primeiro evento anomalo do batch ate o alerta (pior caso)
                lat_primeiro = (recebido - alerta["timestamp_envio_primeiro"]) * 1000
                # do ultimo evento anomalo do batch ate o alerta (melhor caso)
                lat_ultimo = (recebido - alerta["timestamp_envio_ultimo"]) * 1000
                # so o trecho Spark -> Kafka -> este consumidor
                lat_kafka = (recebido - alerta["timestamp_disparo"]) * 1000

                latencias.append(lat_primeiro)
                f.write(f"{alerta['poco_id']},{alerta['regra']},{lat_primeiro:.2f},"
                        f"{lat_ultimo:.2f},{lat_kafka:.2f}\n")
                print(f"{alerta['poco_id']} {alerta['regra']} latencia={lat_primeiro:.1f} ms")

    f.close()
    consumer.close()

    print("\n===== RESULTADO =====")
    print(f"Alertas recebidos: {len(latencias)}")
    if len(latencias) > 0:
        print(f"Media : {np.mean(latencias):.1f} ms")
        print(f"P50   : {np.percentile(latencias, 50):.1f} ms")
        print(f"P95   : {np.percentile(latencias, 95):.1f} ms")
        print(f"Maximo: {np.max(latencias):.1f} ms")
    print(f"Arquivo salvo: {arquivo}")


if __name__ == "__main__":
    main()