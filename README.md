# Pipeline de Manutenção Preditiva - Dataset 3W Petrobras

Projeto de Engenharia de Dados e Streaming em Tempo Real desenvolvido para monitorização, processamento e detecção de anomalias em poços de petróleo utilizando **Apache Kafka**, **Docker** e **Apache Spark**.

---

## Arquitetura do Sistema

1. **Módulo Produtor (Kafka Ingestion):** Leitura recursiva dos ficheiros Parquet do Dataset 3W (`data/raw/`), tratamento de nulos, enriquecimento de metadados e transmissão em streaming no formato JSON.
2. **Broker de Mensagens (Apache Kafka):** Cluster em contêiner Docker gerindo o tópico `telemetria-pocos`.
3. **Módulo Consumidor (Spark Structured Streaming):** Processamento em tempo real, janelamento de dados e aplicação de modelos preditivos (MLlib).

---

## Módulo 1: Produtor Kafka (Ingestão de Dados)

O módulo produtor (`src/produtor/produtor_3w.py`) é responsável por simular a telemetria em tempo real dos sensores dos poços de petróleo.

### Características do Produtor:
- **Varredura Recursiva:** Navega automaticamente por todas as subpastas de classes de anomalia (`0` a `9`).
- **Tratamento de Nulos:** Conversão automática de valores `NaN` para `None` (garantindo JSON válido).
- **Enriquecimento de Metadados:** Adiciona `timestamp_envio`, `poco_id` (ex: `WELL-00001`) e `arquivo_origem` a cada mensagem.
- **Taxa de Transferência Configurável:** Suporta ajuste de delay entre envios e limite de amostragem de ficheiros.

### Estrutura da Mensagem JSON (Tópico: `telemetria-pocos`)

```json
{
  "timestamp_envio": 1730302244.123,
  "arquivo_origem": "WELL-00001_20170201010207.parquet",
  "poco_id": "WELL-00001",
  "P-PDG": 452.12,
  "P-TP": 120.5,
  "T-TP": 65.3,
  "P-MON-CK": 12.4,
  "T-JSC": 45.1,
  "class": 0
}