"""
Gera os graficos do relatorio a partir dos CSVs da pasta resultados/ (Pessoa 4).

Uso:
    python -m src.benchmarks.gerar_graficos
"""

import glob
import json
import os

import matplotlib
matplotlib.use("Agg")  # salva as imagens sem abrir janela
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PASTA = "resultados"
PASTA_GRAFICOS = "resultados/graficos"


def ler_csvs(prefixo):
    # le arquivos como resultados/latencia_taxa_100.csv e devolve {100: DataFrame}
    dados = {}
    for caminho in glob.glob(f"{PASTA}/{prefixo}_taxa_*.csv"):
        taxa = os.path.basename(caminho).replace(f"{prefixo}_taxa_", "").replace(".csv", "")
        if taxa.isdigit():
            dados[int(taxa)] = pd.read_csv(caminho)
    return dict(sorted(dados.items()))


def grafico_latencia():
    dados = ler_csvs("latencia")
    if not dados:
        print("Sem dados de latencia")
        return
    taxas = list(dados.keys())
    medias = [dados[t]["latencia_primeiro_ms"].mean() for t in taxas]
    p95 = [np.percentile(dados[t]["latencia_primeiro_ms"], 95) for t in taxas]

    plt.figure(figsize=(7, 4))
    plt.plot(taxas, medias, marker="o", label="Media")
    plt.plot(taxas, p95, marker="s", label="P95")
    plt.xlabel("Throughput (mensagens/s)")
    plt.ylabel("Latencia end-to-end (ms)")
    plt.title("Latencia ate o alerta x Throughput")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{PASTA_GRAFICOS}/latencia_vs_throughput.png", dpi=150)
    plt.close()


def grafico_memoria():
    dados = ler_csvs("memoria")
    if not dados:
        print("Sem dados de memoria")
        return
    taxas = list(dados.keys())
    spark = [dados[t]["spark_mb"].mean() for t in taxas]
    kafka = [dados[t]["kafka_mb"].mean() for t in taxas]

    x = np.arange(len(taxas))
    plt.figure(figsize=(7, 4))
    plt.bar(x - 0.2, spark, width=0.4, label="Spark (JVM)")
    plt.bar(x + 0.2, kafka, width=0.4, label="Kafka (Docker)")
    plt.xticks(x, taxas)
    plt.xlabel("Throughput (mensagens/s)")
    plt.ylabel("Memoria media (MB)")
    plt.title("Consumo de memoria x Throughput")
    plt.legend()
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{PASTA_GRAFICOS}/memoria_vs_throughput.png", dpi=150)
    plt.close()


def grafico_batches():
    dados = ler_csvs("batches")
    if not dados:
        print("Sem dados de batches")
        return
    taxas = list(dados.keys())

    # tempo medio de processamento de cada micro-batch
    duracao = [dados[t]["duracao_ms"].mean() for t in taxas]
    plt.figure(figsize=(7, 4))
    plt.plot(taxas, duracao, marker="o", color="tab:green")
    plt.xlabel("Throughput (mensagens/s)")
    plt.ylabel("Tempo medio do micro-batch (ms)")
    plt.title("Tempo de processamento do micro-batch x Throughput")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{PASTA_GRAFICOS}/duracao_batch_vs_throughput.png", dpi=150)
    plt.close()

    # quantas anomalias viraram alerta (efeito do motor de regras)
    anomalias = [dados[t]["anomalias"].sum() for t in taxas]
    alertas = [dados[t]["alertas"].sum() for t in taxas]
    x = np.arange(len(taxas))
    plt.figure(figsize=(7, 4))
    plt.bar(x - 0.2, anomalias, width=0.4, label="Anomalias detectadas")
    plt.bar(x + 0.2, alertas, width=0.4, label="Alertas emitidos")
    plt.xticks(x, taxas)
    plt.xlabel("Throughput (mensagens/s)")
    plt.ylabel("Quantidade")
    plt.title("Anomalias detectadas x Alertas emitidos")
    plt.legend()
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{PASTA_GRAFICOS}/anomalias_vs_alertas.png", dpi=150)
    plt.close()


def grafico_modelo():
    caminho = "models/metricas_modelo.json"
    if not os.path.exists(caminho):
        print("Sem metricas do modelo (rode python -m src.ml.treinar_modelo)")
        return
    with open(caminho, encoding="utf-8") as f:
        metricas = json.load(f)

    # barras com as metricas gerais
    nomes = ["Accuracy", "F1-Score", "Precision", "Recall"]
    valores = [metricas["accuracy"], metricas["f1_score"],
               metricas["weighted_precision"], metricas["weighted_recall"]]
    plt.figure(figsize=(6, 4))
    plt.bar(nomes, valores, color="tab:purple")
    for i, v in enumerate(valores):
        plt.text(i, v + 0.01, f"{v:.3f}", ha="center")
    plt.ylim(0, 1.1)
    plt.title("Metricas do modelo (conjunto de teste)")
    plt.tight_layout()
    plt.savefig(f"{PASTA_GRAFICOS}/metricas_modelo.png", dpi=150)
    plt.close()

    # matriz de confusao (0 = normal, 1 = iminente, 2 = pontual)
    matriz = np.zeros((3, 3), dtype=int)
    for item in metricas["matriz_confusao"]:
        matriz[item["label_real"]][item["predicao"]] = item["contagem"]

    rotulos = ["Normal", "Iminente", "Pontual"]
    plt.figure(figsize=(5, 4))
    plt.imshow(matriz, cmap="Blues")
    plt.colorbar()
    plt.xticks(range(3), rotulos)
    plt.yticks(range(3), rotulos)
    plt.xlabel("Predito")
    plt.ylabel("Real")
    for i in range(3):
        for j in range(3):
            cor = "white" if matriz[i][j] > matriz.max() / 2 else "black"
            plt.text(j, i, matriz[i][j], ha="center", va="center", color=cor)
    plt.title("Matriz de confusao")
    plt.tight_layout()
    plt.savefig(f"{PASTA_GRAFICOS}/matriz_confusao.png", dpi=150)
    plt.close()


def imprimir_tabela():
    # tabela resumo para colocar no relatorio
    lat = ler_csvs("latencia")
    mem = ler_csvs("memoria")
    print("\nTaxa | Alertas | Lat. media (ms) | Lat. P95 (ms) | Spark (MB) | Kafka (MB)")
    for taxa in lat:
        l = lat[taxa]["latencia_primeiro_ms"]
        spark = mem[taxa]["spark_mb"].mean() if taxa in mem else 0
        kafka = mem[taxa]["kafka_mb"].mean() if taxa in mem else 0
        print(f"{taxa} | {len(l)} | {l.mean():.1f} | {np.percentile(l, 95):.1f} | {spark:.0f} | {kafka:.0f}")


def main():
    os.makedirs(PASTA_GRAFICOS, exist_ok=True)
    grafico_latencia()
    grafico_memoria()
    grafico_batches()
    grafico_modelo()
    imprimir_tabela()
    print(f"\nGraficos salvos em {PASTA_GRAFICOS}/")


if __name__ == "__main__":
    main()