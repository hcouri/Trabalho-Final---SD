"""
Monitor de memoria durante os benchmarks (Pessoa 4).

A cada segundo registra:
  - memoria do processo Java do Spark (via psutil)
  - memoria do conteiner do Kafka (via docker stats)

Uso:
    python -m src.benchmarks.monitor_memoria --taxa 100 --duracao 150
"""

import argparse
import os
import subprocess
import time

import psutil


def memoria_spark_mb():
    # o Spark roda numa JVM: procura processos java com "spark" na linha de comando
    total = 0
    for proc in psutil.process_iter(["name", "cmdline", "memory_info"]):
        try:
            nome = proc.info["name"] or ""
            comando = " ".join(proc.info["cmdline"] or []).lower()
            if "java" in nome.lower() and "spark" in comando:
                total += proc.info["memory_info"].rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total / (1024 * 1024)


def converter_para_mb(texto):
    # exemplos de entrada: "512.3MiB", "1.2GiB", "800kB"
    texto = texto.strip()
    unidades = {"GiB": 1024, "GB": 1000, "MiB": 1, "MB": 1, "KiB": 1 / 1024, "kB": 1 / 1000, "B": 1 / (1024 * 1024)}
    for unidade, fator in unidades.items():
        if texto.endswith(unidade):
            return float(texto.replace(unidade, "")) * fator
    return 0.0


def memoria_kafka_mb(container):
    try:
        saida = subprocess.run(
            ["docker", "stats", container, "--no-stream", "--format", "{{.MemUsage}}"],
            capture_output=True, text=True, timeout=10,
        ).stdout
        # saida tem o formato "512.3MiB / 7.6GiB"; pega so a parte usada
        return converter_para_mb(saida.split("/")[0])
    except Exception:
        return 0.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--taxa", default="manual", help="nome do experimento (usado no arquivo)")
    parser.add_argument("--duracao", type=int, default=150)
    parser.add_argument("--container", default="kafka-3w", help="nome do conteiner do Kafka")
    args = parser.parse_args()

    os.makedirs("resultados", exist_ok=True)
    arquivo = f"resultados/memoria_taxa_{args.taxa}.csv"
    f = open(arquivo, "w", encoding="utf-8")
    f.write("segundo,spark_mb,kafka_mb\n")

    inicio = time.time()
    while time.time() - inicio < args.duracao:
        segundo = time.time() - inicio
        spark = memoria_spark_mb()
        kafka = memoria_kafka_mb(args.container)
        f.write(f"{segundo:.1f},{spark:.1f},{kafka:.1f}\n")
        f.flush()
        print(f"t={segundo:.0f}s Spark={spark:.0f} MB Kafka={kafka:.0f} MB")
        time.sleep(1)

    f.close()
    print(f"Arquivo salvo: {arquivo}")


if __name__ == "__main__":
    main()