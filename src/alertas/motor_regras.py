"""
Motor de regras de alertas (Pessoa 4).

Recebe as predicoes do pipeline da Pessoa 3 e decide quais anomalias
viram alerta no topico alertas-operacionais.

Regras:
  R1 - Critica imediata: evento pontual com score ML alto e 2+ sensores acima de 3 sigma
  R2 - Persistencia: 5+ anomalias do poco na janela, sendo 2+ pontuais
  R3 - Tendencia iminente: 10+ anomalias iminentes do poco na janela
Depois de um alerta, o poco fica em cooldown (sem novos alertas), a nao ser que piore.
"""

import json
import time

from pyspark.sql import functions as F
from kafka import KafkaProducer

# Limiares das regras
SCORE_CRITICO = 0.85
MIN_SENSORES_3SIGMA = 2
JANELA_SEGUNDOS = 30
MIN_ANOMALIAS_JANELA = 5
MIN_PONTUAIS_JANELA = 2
MIN_IMINENTES_JANELA = 10
COOLDOWN_SEGUNDOS = 60


def resumir_por_poco(batch_df):
    """Agrupa o micro-batch por poco (feito no Spark) e devolve uma lista de dicionarios."""
    pontual = F.col("status_operacional") == "ANOMALIA_PONTUAL"
    iminente = F.col("status_operacional") == "ANOMALIA_IMINENTE"
    anomalia = F.col("status_operacional") != "NORMAL"
    critico = (
        pontual
        & (F.col("score_anomalia_ml") >= SCORE_CRITICO)
        & (F.col("qtd_sensores_fora_3sigma") >= MIN_SENSORES_3SIGMA)
    )

    resumo = batch_df.groupBy("poco_id").agg(
        F.count("*").alias("eventos"),
        F.sum(F.when(critico, 1).otherwise(0)).alias("criticos"),
        F.sum(F.when(pontual, 1).otherwise(0)).alias("pontuais"),
        F.sum(F.when(iminente, 1).otherwise(0)).alias("iminentes"),
        F.max("score_anomalia_ml").alias("score_max"),
        F.max("z_score_max").alias("z_max"),
        # considera apenas o timestamp dos eventos anomalos
        F.min(F.when(anomalia, F.col("timestamp_envio"))).alias("envio_primeiro"),
        F.max(F.when(anomalia, F.col("timestamp_envio"))).alias("envio_ultimo"),
        # diagnostico do evento com maior score do batch
        F.max_by("diagnostico_padrao", "score_anomalia_ml").alias("diagnostico"),
    )
    resumo = resumo.filter((F.col("pontuais") + F.col("iminentes")) > 0)

    lista = []
    for linha in resumo.collect():
        lista.append(linha.asDict())
    return lista


class MotorAlertas:

    def __init__(self, cooldown=COOLDOWN_SEGUNDOS):
        self.cooldown = cooldown
        self.historico = {}       # poco -> lista de (instante, anomalias, pontuais, iminentes)
        self.ultimo_alerta = {}   # poco -> (instante, prioridade)
        self.total_alertas = 0
        self.total_suprimidos = 0

    def atualizar_historico(self, poco, agora, linha):
        lista = self.historico.get(poco, [])
        anomalias = linha["pontuais"] + linha["iminentes"]
        lista.append((agora, anomalias, linha["pontuais"], linha["iminentes"]))

        # mantem apenas o que esta dentro da janela
        lista = [item for item in lista if agora - item[0] <= JANELA_SEGUNDOS]
        self.historico[poco] = lista

        total_anomalias = sum(item[1] for item in lista)
        total_pontuais = sum(item[2] for item in lista)
        total_iminentes = sum(item[3] for item in lista)
        return total_anomalias, total_pontuais, total_iminentes

    def escolher_regra(self, linha, anomalias, pontuais, iminentes):
        # retorna (regra, prioridade, severidade); prioridade 1 e a mais grave
        if linha["criticos"] >= 1:
            return "R1_CRITICA_IMEDIATA", 1, "CRITICA"
        if anomalias >= MIN_ANOMALIAS_JANELA and pontuais >= MIN_PONTUAIS_JANELA:
            return "R2_PERSISTENCIA", 2, "CRITICA"
        if iminentes >= MIN_IMINENTES_JANELA:
            return "R3_TENDENCIA_IMINENTE", 3, "ALERTA_PREVENTIVO"
        return None, None, None

    def esta_em_cooldown(self, poco, prioridade, agora):
        if poco not in self.ultimo_alerta:
            return False
        instante, prioridade_anterior = self.ultimo_alerta[poco]
        if prioridade < prioridade_anterior:
            return False  # a situacao piorou, entao o alerta deve sair
        return agora - instante < self.cooldown

    def avaliar(self, resumo, batch_id):
        agora = time.time()
        alertas = []

        for linha in resumo:
            poco = linha["poco_id"]
            anomalias, pontuais, iminentes = self.atualizar_historico(poco, agora, linha)
            regra, prioridade, severidade = self.escolher_regra(linha, anomalias, pontuais, iminentes)

            if regra is None:
                continue
            if self.esta_em_cooldown(poco, prioridade, agora):
                self.total_suprimidos += 1
                continue

            self.ultimo_alerta[poco] = (agora, prioridade)
            alertas.append({
                "batch_id": batch_id,
                "poco_id": poco,
                "regra": regra,
                "prioridade": prioridade,
                "severidade": severidade,
                "diagnostico": linha["diagnostico"],
                "eventos_batch": linha["eventos"],
                "anomalias_janela": anomalias,
                "pontuais_janela": pontuais,
                "iminentes_janela": iminentes,
                "score_max": linha["score_max"],
                "z_max": round(linha["z_max"], 3),
                "timestamp_envio_primeiro": linha["envio_primeiro"],
                "timestamp_envio_ultimo": linha["envio_ultimo"],
            })

        self.total_alertas += len(alertas)
        return alertas


def criar_produtor_alertas(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def publicar_alertas(producer, topico, alertas):
    """Envia os alertas ao Kafka. Se producer for None, apenas imprime (usado no teste)."""
    for alerta in alertas:
        # marca o momento do disparo logo antes de enviar
        alerta["timestamp_disparo"] = time.time()

        if producer is not None:
            producer.send(topico, key=alerta["poco_id"], value=alerta)

        print(f"  [ALERTA {alerta['severidade']}] {alerta['poco_id']} - {alerta['regra']} - {alerta['diagnostico']}")

    if producer is not None and len(alertas) > 0:
        producer.flush()