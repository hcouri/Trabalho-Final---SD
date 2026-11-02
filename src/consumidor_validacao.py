import json
import time
import numpy as np
from kafka import KafkaConsumer
from setup_kafka import BOOTSTRAP_SERVERS, TOPICO_TELEMETRIA, validar_mensagem_telemetria

def iniciar_consumidor_teste(limite_mensagens=100):
    print(f"Iniciando Consumidor de Validação no tópico '{TOPICO_TELEMETRIA}'...")
    
    consumer = KafkaConsumer(
        TOPICO_TELEMETRIA,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='grupo-validacao-pessoa2',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    latencias_ms = []
    mensagens_validas = 0
    mensagens_invalidas = 0

    print("Aguardando mensagens... (Pressione Ctrl+C para encerrar e gerar o relatório)\n")

    try:
        for mensagem in consumer:
            tempo_recebimento = time.time()
            dados = mensagem.value

            valido, motivo = validar_mensagem_telemetria(dados)

            if valido:
                mensagens_validas += 1
                # Calcula a latência de rede/broker em milissegundos
                latencia = (tempo_recebimento - dados["timestamp_envio"]) * 1000.0
                latencias_ms.append(latencia)

                if mensagens_validas % 10 == 0 or mensagens_validas == 1:
                    print(f"[Msg #{mensagens_validas} | Partição {mensagem.partition}] "
                          f"Origem: {dados['arquivo_origem']} | "
                          f"Sensores lidos: {len(dados['sensores'])} | "
                          f"Latência: {latencia:.2f} ms")
            else:
                mensagens_invalidas += 1
                print(f"[ERRO DE INTEGRIDADE] Partição {mensagem.partition}: {motivo}")

            if mensagens_validas + mensagens_invalidas >= limite_mensagens:
                print(f"\nLimite de {limite_mensagens} mensagens atingido.")
                break

    except KeyboardInterrupt:
        print("\nValidação interrompida pelo usuário.")
    finally:
        consumer.close()
        exibir_relatorio(mensagens_validas, mensagens_invalidas, latencias_ms)

def exibir_relatorio(validas, invalidas, latencias):
    print("\n" + "="*45)
    print(" RELATÓRIO DE VALIDAÇÃO E LATÊNCIA (PESSOA 2)")
    print("="*45)
    print(f"Total de mensagens analisadas : {validas + invalidas}")
    print(f"Mensagens íntegras (válidas)  : {validas}")
    print(f"Mensagens corrompidas/erro    : {invalidas}")
    
    if latencias:
        print(f"Latência Média de Entrega     : {np.mean(latencias):.2f} ms")
        print(f"Latência Mínima / Máxima      : {np.min(latencias):.2f} ms / {np.max(latencias):.2f} ms")
        print(f"Percentil 95 (P95)            : {np.percentile(latencias, 95):.2f} ms")
    print("="*45)

if __name__ == "__main__":
    iniciar_consumidor_teste(limite_mensagens=200)