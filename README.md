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
- **Enriquecimento de Metados:** Adiciona `timestamp_envio`, `poco_id` (ex: `WELL-00001`) e `arquivo_origem` a cada mensagem.
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
```

---

## Módulo 3: Consumo em Spark Streaming e Pipeline de ML (Pessoa 3)

O Módulo 3 realiza o consumo em tempo real do tópico Kafka `telemetria-pocos` via **PySpark Structured Streaming**, aplica a camada de **normalização e pré-processamento online** com os parâmetros estatísticos validados no Trabalho 2 e executa o pipeline preditivo em **`pyspark.ml`** para reconhecimento de padrões operacionais e detecção de anomalias pontuais ou iminentes.

### Estrutura de Arquivos do Módulo 3
- `configs/parametros_estatisticos_t2.json`: Parâmetros estatísticos de referência ($\mu$, $\sigma$, mediana para imputação de nulos, mínimo, máximo e catálogo de falhas 0–9 e 101–109 do Dataset 3W).
- `src/streaming/gerar_parametros_t2.py`: Utilitário em lote (reutilizando o padrão de *streaming/batching* do Trabalho 2) para recalcular e validar incrementalmente as estatísticas diretamente dos arquivos Parquet em `data/raw/`.
- `src/streaming/preprocessamento.py`:
  - **Parser Unificado Kafka (`parsear_stream_kafka`):** Compatível simultaneamente com o formato JSON plano do produtor da Pessoa 1 (`src/produtor/produtor_3w.py`) e com o formato aninhado `sensores` da Pessoa 2 (`src/setup_kafka.py`).
  - **Pré-processamento e Normalização Online (`aplicar_preprocessamento_online`):** Imputação de leituras ausentes (`NaN`/`null`), normalização Z-Score online adaptativa por sensor (`P-PDG`, `P-TPT`, `T-TPT`, `P-MON-CKP`, `T-JUS-CKP`, `P-JUS-CKGL`, `T-JUS-CKGL`, `QGL`), cálculo de `z_score_medio`, `z_score_max`, contagem de desvios $> 2\sigma$ (alerta iminente) e $> 3\sigma$ (anomalia crítica), além de diferenciais termohidráulicos de pressão e temperatura.
- `src/ml/treinar_modelo.py`:
  - Constrói, treina, avalia (sem *data leakage*, com *split* 80/20 prévio) e persiste o `pyspark.ml.Pipeline` em `models/pipeline_ml_3w`, composto por:
    1. `Imputer` (estratégia mediana para eventuais nulos residuais)
    2. `VectorAssembler` (14 atributos normalizados e estatísticos)
    3. `StandardScaler` (`withMean=True`, `withStd=True`)
    4. `KMeans` ($k=4$, reconhecimento não-supervisionado de regimes operacionais do poço)
    5. `RandomForestClassifier` (60 árvores, classificação multiclasse em `0 = NORMAL`, `1 = ANOMALIA_IMINENTE`, `2 = ANOMALIA_PONTUAL`)
- `src/streaming/consumidor_spark_ml.py`:
  - Assina o tópico `telemetria-pocos` com `spark.readStream.format("kafka")`, aplica o pré-processamento online e o `PipelineModel` em cada micro-batch, calcula a latência *end-to-end* (`latencia_end_to_end_ms`) e permite publicar automaticamente anomalias detectadas no tópico `alertas-operacionais` (`--publicar-alertas`).
- `src/streaming/teste_pipeline_p3.py`:
  - Script de validação autônoma fim a fim do pipeline Spark + ML sem depender de broker Kafka externo.

### Como Executar o Módulo 3

1. **(Opcional) Treinar / Atualizar o Modelo `pyspark.ml` Offline:**
   ```bash
   python -m src.ml.treinar_modelo
   ```

2. **Executar o Teste Integrado Autônomo (Validação Local do Pipeline):**
   ```bash
   python -m src.streaming.teste_pipeline_p3
   ```

3. **Iniciar o Consumidor Spark Structured Streaming Conectado ao Kafka:**
   ```bash
   python -m src.streaming.consumidor_spark_ml --bootstrap-servers localhost:9092 --topico-entrada telemetria-pocos --publicar-alertas
   ```
