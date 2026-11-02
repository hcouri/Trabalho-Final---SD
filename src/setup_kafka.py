import json
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPICO_TELEMETRIA = 'telemetria-pocos'
TOPICO_ALERTAS = 'alertas-operacionais'

# Esquema oficial (Contrato de Dados entre Pessoa 1, 2 e 3)
CAMPOS_OBRIGATORIOS = {
    "timestamp_evento": str,      # Data/hora original da linha no dataset 3W
    "timestamp_envio": float,     # time.time() no momento do disparo pelo Produtor
    "arquivo_origem": str,        # Ex: WELL-00002_20170211080129.parquet
    "class": (int, float, type(None)), # Código de falha (0 = normal, 1..8 = anomalia)
    "sensores": dict              # Dicionário com { "P-PDG": valor, "T-TPT": valor, ... }
}

def criar_topicos():
    print("Conectando ao cluster Kafka...")
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id='admin-pessoa2'
    )

    topicos = [
        # 4 partições permitem que o Spark Streaming distribua a leitura em 4 núcleos
        NewTopic(name=TOPICO_TELEMETRIA, num_partitions=4, replication_factor=1),
        NewTopic(name=TOPICO_ALERTAS, num_partitions=1, replication_factor=1)
    ]

    for topico in topicos:
        try:
            admin_client.create_topics(new_topics=[topico], validate_only=False)
            print(f"[SUCESSO] Tópico '{topico.name}' criado ({topico.num_partitions} partições).")
        except TopicAlreadyExistsError:
            print(f"[INFO] Tópico '{topico.name}' já existe.")
        except Exception as e:
            print(f"[ERRO] Falha ao criar '{topico.name}': {e}")

    admin_client.close()

def validar_mensagem_telemetria(payload_dict):
    """Verifica se o JSON recebido respeita o contrato de dados do grupo."""
    if not isinstance(payload_dict, dict):
        return False, "Payload não é um objeto JSON válido."

    for campo, tipo_esperado in CAMPOS_OBRIGATORIOS.items():
        if campo not in payload_dict:
            return False, f"Campo obrigatório ausente: '{campo}'"
        if not isinstance(payload_dict[campo], tipo_esperado):
            return False, f"Tipo inválido em '{campo}'. Esperado {tipo_esperado}, recebido {type(payload_dict[campo])}"

    if len(payload_dict["sensores"]) == 0:
        return False, "O dicionário de 'sensores' está vazio."

    return True, "OK"

if __name__ == "__main__":
    criar_topicos()