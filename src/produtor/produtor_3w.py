import json
import time
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer

# Configurações do Cluster Kafka
KAFKA_BROKER = "localhost:9092"
TOPICO_DESTINO = "telemetria-pocos"
INTERVALO_LEITURA_SEGUNDOS = 1.0  # Taxa real de 1 Hz (1 leitura por segundo por poço)


def criar_produtor():
    """Inicializa e retorna o cliente KafkaProducer configurado para JSON."""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
    )


def carregar_fluxos_por_evento(path_raw: Path, arquivos_por_classe: int = 1):
    """
    Carrega uma amostra estratificada de arquivos (0 = normal, 1..8 = anomalias)
    e prepara os iteradores para transmissão simultânea em paralelo.
    """
    fluxos = []
    subpastas = sorted([p for p in path_raw.iterdir() if p.is_dir()])

    arquivos_selecionados = []
    if subpastas:
        for pasta in subpastas:
            arquivos_selecionados.extend(sorted(pasta.glob("*.parquet"))[:arquivos_por_classe])
    else:
        arquivos_selecionados = sorted(path_raw.glob("*.parquet"))[:9]

    for arquivo in arquivos_selecionados:
        try:
            df = pd.read_parquet(arquivo)
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
            df = df.where(pd.notnull(df), None)

            poco_id = arquivo.name.split("_")[0] if "_" in arquivo.name else "POCO_GENERICO"
            classe_evento = arquivo.parent.name

            fluxos.append({
                "arquivo": arquivo.name,
                "poco_id": poco_id,
                "evento_classe": classe_evento,
                "iterador": df.iterrows()
            })
        except Exception as e:
            print(f"Erro ao carregar {arquivo.name}: {e}")

    return fluxos


def transmitir_telemetria_simultanea(diretorio_raw: str, arquivos_por_classe: int = 1):
    """
    Simula múltiplos poços transmitindo telemetria simultaneamente a 1 Hz.
    A cada 1 segundo, envia a leitura correspondente daquele instante de cada poço ativo.
    """
    path_raw = Path(diretorio_raw)
    fluxos_ativos = carregar_fluxos_por_evento(path_raw, arquivos_por_classe)

    if not fluxos_ativos:
        print(f"Nenhum ficheiro .parquet válido encontrado em: {path_raw.resolve()}")
        return

    print(f"Poços/Eventos ativos em paralelo: {len(fluxos_ativos)} fluxos (normal + anomalias).")
    print(f"Cadência de aquisição: {INTERVALO_LEITURA_SEGUNDOS}s por ciclo (1 Hz por poço).\n")

    producer = criar_produtor()
    total_mensagens = 0
    segundo_simulacao = 0

    try:
        while fluxos_ativos:
            segundo_simulacao += 1
            fluxos_restantes = []

            for fluxo in fluxos_ativos:
                try:
                    _, row = next(fluxo["iterador"])
                    dado = row.to_dict()

                    # Metadados essenciais para rastreabilidade e correlação no Spark
                    dado["timestamp_envio"] = time.time()
                    dado["arquivo_origem"] = fluxo["arquivo"]
                    dado["poco_id"] = fluxo["poco_id"]
                    dado["evento_classe"] = fluxo["evento_classe"]

                    producer.send(TOPICO_DESTINO, value=dado)
                    total_mensagens += 1
                    fluxos_restantes.append(fluxo)

                except StopIteration:
                    print(f"Fluxo finalizado: {fluxo['arquivo']} (Evento '{fluxo['evento_classe']}')")

            fluxos_ativos = fluxos_restantes

            if fluxos_ativos:
                producer.flush()
                print(f"[Segundo {segundo_simulacao}s] Enviadas {len(fluxos_ativos)} leituras simultâneas | Total acumulado: {total_mensagens}")
                time.sleep(INTERVALO_LEITURA_SEGUNDOS)

    except KeyboardInterrupt:
        print("\nTransmissão interrompida manualmente pelo operador.")
    finally:
        producer.flush()
        producer.close()
        print(f"Produtor encerrado com segurança. Total de mensagens entregues ao Kafka: {total_mensagens}")


if __name__ == "__main__":
    pasta_data_raw = "data/raw"
    print(f"A iniciar o Produtor Kafka Multi-Poço (Tópico: {TOPICO_DESTINO})...")
    transmitir_telemetria_simultanea(pasta_data_raw, arquivos_por_classe=1)