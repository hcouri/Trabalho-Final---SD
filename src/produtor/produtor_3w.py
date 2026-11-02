import json
import time
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer

# Configurações do Cluster Kafka
KAFKA_BROKER = "localhost:9092"
TOPICO_DESTINO = "telemetria-pocos"


def criar_produtor():
    """Inicializa e retorna o cliente KafkaProducer configurado para JSON."""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def transmitir_pasta_raw(diretorio_raw: str, delay_entre_linhas: float = 0.0001, max_arquivos: int = 15):
    """
    Varre a pasta data/raw e subpastas enviando os dados dos ficheiros Parquet para o Kafka.
    
    :param diretorio_raw: Caminho para a pasta de dados brutos.
    :param delay_entre_linhas: Intervalo de tempo (em segundos) entre o envio de cada linha.
    :param max_arquivos: Limite de ficheiros a processar para testes rápidos.
    """
    path_raw = Path(diretorio_raw)
    
    # Procura recursivamente todos os ficheiros .parquet nas subpastas (0 a 9)
    arquivos_parquet = list(path_raw.glob("**/*.parquet"))
    
    if not arquivos_parquet:
        print(f"Nenhum ficheiro .parquet encontrado em: {path_raw.resolve()}")
        return

    # Seleciona apenas o limite especificado para amostragem
    arquivos_para_enviar = arquivos_parquet[:max_arquivos]
    
    print(f"Ficheiros encontrados no total: {len(arquivos_parquet)}")
    print(f"Limite configurado para envio: {len(arquivos_para_enviar)} ficheiros.\n")
    
    producer = criar_produtor()
    total_mensagens = 0

    for idx, arquivo in enumerate(arquivos_para_enviar, start=1):
        print(f"[{idx}/{len(arquivos_para_enviar)}] A processar ficheiro: {arquivo.name}...")
        
        try:
            df = pd.read_parquet(arquivo)
            # Substitui valores NaN por None para gerar JSON válido
            df = df.where(pd.notnull(df), None)

            # Extrai o ID do poço a partir do nome do ficheiro (ex: WELL-00001)
            poco_id = arquivo.name.split("_")[0] if "_" in arquivo.name else "POCO_GENERICO"

            for _, row in df.iterrows():
                dado = row.to_dict()
                
                # Adiciona metadados essenciais para o Spark e rastreabilidade
                dado["timestamp_envio"] = time.time()
                dado["arquivo_origem"] = arquivo.name
                dado["poco_id"] = poco_id

                producer.send(TOPICO_DESTINO, value=dado)
                total_mensagens += 1
                
                if delay_entre_linhas > 0:
                    time.sleep(delay_entre_linhas)

            producer.flush()
            print(f"{arquivo.name} transmitido com sucesso.")

        except Exception as e:
            print(f"Erro ao processar o ficheiro {arquivo.name}: {e}")

    print(f"\nCarga finalizada com sucesso!")
    print(f"Total de mensagens enviadas ao Kafka: {total_mensagens}")


if __name__ == "__main__":
    pasta_data_raw = "data/raw"
    print(f"A iniciar o Produtor Kafka (Tópico: {TOPICO_DESTINO})...")
    
    # Para enviar mais ficheiros, altera o parâmetro max_arquivos
    transmitir_pasta_raw(pasta_data_raw, delay_entre_linhas=0.0001, max_arquivos=15)