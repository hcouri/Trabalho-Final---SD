"""
Utilitário para extração e validação incremental dos parâmetros estatísticos
do Dataset 3W (reutilizando a leitura em batches do Trabalho 2).

Caso a pasta data/raw possua arquivos Parquet do 3W, recalcula média,
desvio padrão, mediana, mínimo e máximo com baixo consumo de memória e
atualiza o arquivo configs/parametros_estatisticos_t2.json.
"""

import json
import math
from pathlib import Path
import pandas as pd  # pyrefly: ignore[missing-import]

CAMINHO_CONFIG_PADRAO = Path("configs/parametros_estatisticos_t2.json")
SENSORES_3W = [
    "P-PDG",
    "P-TPT",
    "T-TPT",
    "P-MON-CKP",
    "T-JUS-CKP",
    "P-JUS-CKGL",
    "T-JUS-CKGL",
    "QGL",
]


def calcular_estatisticas_em_batches(diretorio_raw: str = "data/raw", max_arquivos: int = 200):
    """
    Varre os arquivos Parquet em lotes (streaming/batching do Trabalho 2)
    acumulando soma, soma dos quadrados, contagem, mínimo, máximo e medianas por lote,
    evitando carregar todo o dataset 3W na memória RAM.
    """
    path_raw = Path(diretorio_raw)
    arquivos = list(path_raw.glob("**/*.parquet"))

    if not arquivos:
        print(
            f"[INFO] Nenhum arquivo Parquet encontrado em '{path_raw.resolve()}'. "
            f"Mantendo os parâmetros estatísticos validados padrão em '{CAMINHO_CONFIG_PADRAO}'."
        )
        return None

    arquivos_selecionados = arquivos[:max_arquivos]
    print(f"[INFO] Calculando estatísticas incrementais em {len(arquivos_selecionados)} arquivos...")

    acumuladores = {
        s: {
            "n": 0,
            "soma": 0.0,
            "soma_quad": 0.0,
            "min": float("inf"),
            "max": float("-inf"),
            "medianas_lote": [],
        }
        for s in SENSORES_3W
    }

    for arquivo in arquivos_selecionados:
        try:
            colunas_disponiveis = pd.read_parquet(arquivo)
            for sensor in SENSORES_3W:
                if sensor not in colunas_disponiveis.columns:
                    continue
                serie = pd.to_numeric(colunas_disponiveis[sensor], errors="coerce").dropna()
                qtd = len(serie)
                if qtd == 0:
                    continue

                acc = acumuladores[sensor]
                acc["n"] += qtd
                acc["soma"] += float(serie.sum())
                acc["soma_quad"] += float((serie ** 2).sum())
                acc["min"] = min(acc["min"], float(serie.min()))
                acc["max"] = max(acc["max"], float(serie.max()))
                acc["medianas_lote"].append(float(serie.median()))
        except Exception as exc:
            print(f"[AVISO] Erro ao ler {arquivo.name}: {exc}")

    with open(CAMINHO_CONFIG_PADRAO, "r", encoding="utf-8") as f:
        config_atual = json.load(f)

    sensores_atualizados = 0
    for sensor, acc in acumuladores.items():
        if acc["n"] > 10:
            media = acc["soma"] / acc["n"]
            variancia = max((acc["soma_quad"] / acc["n"]) - (media ** 2), 1e-6)
            desvio = math.sqrt(variancia)
            mediana_aprox = float(pd.Series(acc["medianas_lote"]).median())

            cfg_sensor = config_atual["sensores"].get(sensor, {})
            cfg_sensor["media"] = round(media, 6)
            cfg_sensor["desvio_padrao"] = round(desvio, 6)
            cfg_sensor["mediana"] = round(mediana_aprox, 6)
            cfg_sensor["min"] = round(acc["min"], 6)
            cfg_sensor["max"] = round(acc["max"], 6)
            config_atual["sensores"][sensor] = cfg_sensor
            sensores_atualizados += 1

    if sensores_atualizados > 0:
        with open(CAMINHO_CONFIG_PADRAO, "w", encoding="utf-8") as f:
            json.dump(config_atual, f, indent=2, ensure_ascii=False)
        print(
            f"[SUCESSO] Parâmetros estatísticos atualizados para {sensores_atualizados} sensores "
            f"em '{CAMINHO_CONFIG_PADRAO}'."
        )

    return config_atual


if __name__ == "__main__":
    calcular_estatisticas_em_batches()

