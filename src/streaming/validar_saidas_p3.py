"""
Validador de Saídas Persistidas (Parquet / CSV / Consola) e Extrator de Métricas Reais (Pessoa 3).

Atende diretamente aos dois itens finais da Pessoa 3 para a apresentação:
1. Verifica se os dados processados (eventos normalizados, agregações/médias por poço,
   alertas de anomalias e métricas de micro-batch) estão gravados corretamente em
   Parquet (`resultados/parquet/`) e CSV (`resultados/`), conferindo integridade
   entre alertas emitidos pelo Spark e alertas recebidos do Kafka.
2. Extrai os números reais consolidados da execução (total de eventos processados,
   latência média e P95, poços com anomalia, redução de ruído e acurácia do modelo)
   e salva em `resultados/resumo_metricas_p3.json` para citação na apresentação.

Uso:
    python -m src.streaming.validar_saidas_p3
"""

import glob
import json
import os
from pathlib import Path
from typing import Any, Dict, Set

import numpy as np
import pandas as pd

PASTA_RESULTADOS = Path("resultados")
PASTA_PARQUET = PASTA_RESULTADOS / "parquet"
CAMINHO_METRICAS_MODELO = Path("models/metricas_modelo.json")
CAMINHO_RESUMO_SAIDA = PASTA_RESULTADOS / "resumo_metricas_p3.json"


def validar_camadas_parquet(diretorio_parquet: Path = PASTA_PARQUET) -> Dict[str, Any]:
    """
    Inspeciona as 3 camadas Parquet geradas por `consumidor_spark_ml.py`:
      - eventos_processados
      - agregacoes_pocos
      - alertas_anomalias
    Valida leitura, colunas esperadas, contagem de registros e ausência de corrupção.
    """
    camadas = {
        "eventos_processados": [
            "poco_id",
            "status_operacional",
            "score_anomalia_ml",
            "z_score_max",
            "latencia_end_to_end_ms",
        ],
        "agregacoes_pocos": [
            "poco_id",
            "total_eventos",
            "total_anomalias",
            "media_p_pdg",
            "score_ml_medio",
            "latencia_media_ms",
        ],
        "alertas_anomalias": [
            "poco_id",
            "status_operacional",
            "severidade",
            "diagnostico_padrao",
            "latencia_end_to_end_ms",
        ],
    }

    relatorio: Dict[str, Any] = {}
    for nome, colunas_obrigatorias in camadas.items():
        caminho = diretorio_parquet / nome
        arquivos = list(caminho.glob("**/*.parquet")) if caminho.exists() else []
        if not arquivos:
            relatorio[nome] = {
                "presente": False,
                "status": "NAO_EXECUTADO_AINDA",
                "arquivos_parquet": 0,
                "registros": 0,
            }
            continue

        try:
            df = pd.read_parquet(caminho)
            colunas_faltantes = [c for c in colunas_obrigatorias if c not in df.columns]
            pocos_unicos = (
                sorted(df["poco_id"].dropna().unique().tolist())
                if "poco_id" in df.columns
                else []
            )
            relatorio[nome] = {
                "presente": True,
                "status": "OK" if not colunas_faltantes and len(df) > 0 else "SCHEMA_INCOMPLETO",
                "arquivos_parquet": len(arquivos),
                "registros": len(df),
                "pocos_unicos": len(pocos_unicos),
                "colunas_faltantes": colunas_faltantes,
            }
        except Exception as exc:
            relatorio[nome] = {
                "presente": True,
                "status": f"ERRO_LEITURA: {exc}",
                "arquivos_parquet": len(arquivos),
                "registros": 0,
            }

    return relatorio


def extrair_metricas_reais_execucao(pasta: Path = PASTA_RESULTADOS) -> Dict[str, Any]:
    """
    Lê os arquivos reais das execuções (50, 100, 500 e 1000 msg/s) na pasta
    `resultados/` e consolida todas as métricas quantitativas verificadas.
    """
    por_taxa: Dict[str, Any] = {}
    total_batches = 0
    total_eventos = 0
    total_anomalias = 0
    total_alertas_spark = 0
    total_alertas_recebidos = 0
    pocos_com_anomalia_global: Set[str] = set()

    def _chave_ordenacao_taxa(caminho: str) -> int:
        nome = os.path.basename(caminho).replace("batches_taxa_", "").replace(".csv", "")
        return int(nome) if nome.isdigit() else 0

    arquivos_batches = sorted(
        glob.glob(str(pasta / "batches_taxa_*.csv")),
        key=_chave_ordenacao_taxa,
    )

    for caminho_batch in arquivos_batches:
        taxa_str = os.path.basename(caminho_batch).replace("batches_taxa_", "").replace(".csv", "")
        df_b = pd.read_csv(caminho_batch)
        caminho_lat = pasta / f"latencia_taxa_{taxa_str}.csv"
        caminho_mem = pasta / f"memoria_taxa_{taxa_str}.csv"

        df_l = pd.read_csv(caminho_lat) if caminho_lat.exists() else pd.DataFrame()
        df_m = pd.read_csv(caminho_mem) if caminho_mem.exists() else pd.DataFrame()

        eventos = int(df_b["eventos"].sum())
        anomalias = int(df_b["anomalias"].sum())
        alertas_spark = int(df_b["alertas"].sum())
        alertas_recebidos = len(df_l)
        duracao_batch_media = float(df_b["duracao_ms"].mean())

        total_batches += len(df_b)
        total_eventos += eventos
        total_anomalias += anomalias
        total_alertas_spark += alertas_spark
        total_alertas_recebidos += alertas_recebidos

        info_taxa: Dict[str, Any] = {
            "taxa_msg_s": int(taxa_str) if taxa_str.isdigit() else taxa_str,
            "micro_batches": len(df_b),
            "eventos_processados": eventos,
            "anomalias_detectadas": anomalias,
            "taxa_anomalia_pct": round((anomalias / max(eventos, 1)) * 100.0, 2),
            "alertas_emitidos_spark": alertas_spark,
            "alertas_recebidos_kafka": alertas_recebidos,
            "integridade_entrega_100pct": alertas_spark == alertas_recebidos,
            "reducao_ruido_pct": round((1.0 - alertas_spark / max(anomalias, 1)) * 100.0, 1),
            "duracao_media_batch_ms": round(duracao_batch_media, 1),
        }

        if not df_l.empty:
            pocos = sorted(df_l["poco_id"].dropna().unique().tolist())
            pocos_com_anomalia_global.update(pocos)
            contagem_regras = df_l["regra"].value_counts().to_dict()
            top_pocos = df_l["poco_id"].value_counts().head(3).to_dict()
            info_taxa.update(
                {
                    "pocos_com_alerta": len(pocos),
                    "latencia_media_ms": round(float(df_l["latencia_primeiro_ms"].mean()), 1),
                    "latencia_p50_ms": round(float(np.percentile(df_l["latencia_primeiro_ms"], 50)), 1),
                    "latencia_p95_ms": round(float(np.percentile(df_l["latencia_primeiro_ms"], 95)), 1),
                    "latencia_max_ms": round(float(df_l["latencia_primeiro_ms"].max()), 1),
                    "latencia_kafka_media_ms": round(
                        float(df_l["latencia_disparo_kafka_ms"].mean()), 2
                    ),
                    "regras_acionadas": {str(k): int(v) for k, v in contagem_regras.items()},
                    "top_3_pocos_alertas": {str(k): int(v) for k, v in top_pocos.items()},
                }
            )

        if not df_m.empty:
            info_taxa.update(
                {
                    "memoria_spark_media_mb": round(float(df_m["spark_mb"].mean()), 1),
                    "memoria_kafka_media_mb": round(float(df_m["kafka_mb"].mean()), 1),
                }
            )

        por_taxa[f"{taxa_str}_msg_s"] = info_taxa

    # Carrega métricas do modelo pyspark.ml se existirem
    metricas_ml: Dict[str, Any] = {}
    if CAMINHO_METRICAS_MODELO.exists():
        with open(CAMINHO_METRICAS_MODELO, "r", encoding="utf-8") as f:
            dados_ml = json.load(f)
            metricas_ml = {
                "algoritmo": dados_ml.get("algoritmo"),
                "accuracy_pct": round(float(dados_ml.get("accuracy", 0.0)) * 100.0, 2),
                "f1_score": dados_ml.get("f1_score"),
                "weighted_precision": dados_ml.get("weighted_precision"),
                "weighted_recall": dados_ml.get("weighted_recall"),
                "total_treino": dados_ml.get("total_treino"),
                "total_teste": dados_ml.get("total_teste"),
            }

    return {
        "consolidado_geral": {
            "total_micro_batches": total_batches,
            "total_eventos_processados": total_eventos,
            "total_anomalias_detectadas": total_anomalias,
            "taxa_anomalia_media_pct": round((total_anomalias / max(total_eventos, 1)) * 100.0, 2),
            "total_alertas_emitidos_spark": total_alertas_spark,
            "total_alertas_recebidos_kafka": total_alertas_recebidos,
            "perda_de_mensagens": total_alertas_spark - total_alertas_recebidos,
            "reducao_ruido_media_pct": round(
                (1.0 - total_alertas_spark / max(total_anomalias, 1)) * 100.0, 1
            ),
            "total_pocos_monitorados_com_anomalia": len(pocos_com_anomalia_global),
        },
        "modelo_ml": metricas_ml,
        "experimentos_por_taxa": por_taxa,
    }


def main():
    print("=" * 80)
    print(" VALIDAÇÃO DE SAÍDAS E EXTRAÇÃO DE MÉTRICAS REAIS (PESSOA 3)")
    print("=" * 80)

    status_parquet = validar_camadas_parquet()
    metricas_reais = extrair_metricas_reais_execucao()

    resultado_completo: Dict[str, Any] = {
        "validacao_parquet": status_parquet,
        **metricas_reais,
    }

    PASTA_RESULTADOS.mkdir(parents=True, exist_ok=True)
    with open(CAMINHO_RESUMO_SAIDA, "w", encoding="utf-8") as f:
        json.dump(resultado_completo, f, indent=2, ensure_ascii=False)

    print("\n[1] Status da Persistência Parquet (`resultados/parquet/`):")
    for camada, info in status_parquet.items():
        print(
            f"  - {camada:<22} | Status: {info['status']:<20} | "
            f"Arquivos: {info['arquivos_parquet']:<3} | Registros: {info['registros']}"
        )

    cg = metricas_reais["consolidado_geral"]
    ml = metricas_reais["modelo_ml"]
    print("\n[2] Números Reais Consolidados da Execução (Prontos para Apresentação):")
    print(f"  - Total de eventos processados : {cg['total_eventos_processados']:,} (em {cg['total_micro_batches']} micro-batches)")
    print(f"  - Anomalias detectadas (ML)    : {cg['total_anomalias_detectadas']:,} ({cg['taxa_anomalia_media_pct']}%)")
    print(f"  - Alertas emitidos / recebidos : {cg['total_alertas_emitidos_spark']:,} / {cg['total_alertas_recebidos_kafka']:,} (perda = {cg['perda_de_mensagens']})")
    print(f"  - Redução de ruído (regras)    : {cg['reducao_ruido_media_pct']}%")
    print(f"  - Poços com anomalia detectada : {cg['total_pocos_monitorados_com_anomalia']} poços")
    if ml:
        print(f"  - Acurácia / F1 do modelo ML   : {ml['accuracy_pct']}% / {ml['f1_score']}")

    print(f"\n[SUCESSO] Resumo salvo em: {CAMINHO_RESUMO_SAIDA.resolve()}")


if __name__ == "__main__":
    main()
