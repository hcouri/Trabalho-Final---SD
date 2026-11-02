import json
import time
from kafka import KafkaProducer
from setup_kafka import BOOTSTRAP_SERVERS, TOPICO_TELEMETRIA

def disparar_teste():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=5
    )

    print("Enviando 50 eventos de telemetria simulados...")
    for i in range(50):
        evento = {
            "timestamp_evento": f"2026-11-02 10:00:{i:02d}",
            "timestamp_envio": time.time(),
            "arquivo_origem": "WELL-00001_TESTE.parquet",
            "class": 0,
            "sensores": {
                "P-PDG": 240.5 + (i * 0.1),
                "T-TPT": 85.2,
                "P-TPT": 150.0,
                "P-MON-CKP": 45.1
            }
        }
        producer.send(TOPICO_TELEMETRIA, value=evento)
        time.sleep(0.05) # 20 mensagens por segundo

    producer.flush()
    producer.close()
    print("Envio concluído com sucesso!")

if __name__ == "__main__":
    disparar_teste()