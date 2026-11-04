"""
Gerador de carga para os benchmarks (Pessoa 4).

Envia eventos de telemetria para o Kafka a uma taxa fixa (mensagens por segundo),
no mesmo formato do produtor da Pessoa 1. Uma parte dos eventos e anomala.

Uso:
    python -m src.benchmarks.gerador_carga --taxa 100 --duracao 120
"""

import argparse
import json
import random
import time

from kafka import KafkaProducer

from src.streaming.preprocessamento import carregar_parametros_estatisticos


def gerar_evento(parametros, poco, anomalo):
    if anomalo:
        desvio = random.choice([-1, 1]) * random.uniform(3.5, 5.0)
        classe = 3
    else:
        desvio = 0
        classe = 0

    evento = {"poco_id": poco, "arquivo_origem": poco + "_BENCHMARK.parquet", "class": classe}
    for sensor, info in parametros["sensores"].items():
        # ruido normal em todos os sensores + desvio quando o evento e anomalo
        z = random.gauss(0, 0.5) + desvio
        evento[sensor] = info["media"] + z * info["desvio_padrao"]
    return evento


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topico", default="telemetria-pocos")
    parser.add_argument("--taxa", type=int, default=100, help="mensagens por segundo")
    parser.add_argument("--duracao", type=int, default=120, help="duracao em segundos")
    parser.add_argument("--pocos", type=int, default=20)
    parser.add_argument("--prob-anomalia", type=float, default=0.02)
    args = parser.parse_args()

    parametros = carregar_parametros_estatisticos()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    lista_pocos = [f"WELL-B{i:02d}" for i in range(1, args.pocos + 1)]

    print(f"Enviando {args.taxa} msg/s durante {args.duracao} s...")
    total = 0

    for segundo in range(args.duracao):
        inicio = time.time()

        # envia as mensagens do segundo atual
        for i in range(args.taxa):
            poco = random.choice(lista_pocos)
            anomalo = random.random() < args.prob_anomalia
            evento = gerar_evento(parametros, poco, anomalo)
            evento["timestamp_envio"] = time.time()
            producer.send(args.topico, value=evento)
            total += 1
        producer.flush()

        # espera o restante do segundo para manter a taxa
        tempo_gasto = time.time() - inicio
        if tempo_gasto < 1:
            time.sleep(1 - tempo_gasto)
        else:
            print(f"Aviso: segundo {segundo} levou {tempo_gasto:.2f} s (taxa nao foi mantida)")

        if (segundo + 1) % 10 == 0:
            print(f"{segundo + 1} s - {total} mensagens enviadas")

    producer.close()
    print(f"Fim. Total enviado: {total}")


if __name__ == "__main__":
    main()