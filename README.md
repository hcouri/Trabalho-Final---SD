# Pipeline de Manutenção Preditiva - Dataset 3W Petrobras

Projeto de Engenharia de Dados e Streaming em Tempo Real desenvolvido para monitorização, processamento, persistência e detecção de anomalias em poços de petróleo utilizando **Apache Kafka**, **Docker** e **Apache Spark**.

---

## Arquitetura do Sistema

```
Dataset 3W (Parquet) -> Produtor Python -> Kafka [telemetria-pocos] -> Spark Streaming + ML -> Agregações/Parquet + Motor de Regras -> Kafka [alertas-operacionais]
```

1. **Módulo 1 — Produtor Kafka (Pessoa 1):** Leitura dos ficheiros Parquet do Dataset 3W (`data/raw/`), tratamento de nulos, enriquecimento de metadados (`poco_id`, `timestamp_envio`, `arquivo_origem`, `evento_classe`) e transmissão em JSON.
2. **Módulo 2 — Infraestrutura Kafka & Resiliência (Pessoa 2):** Cluster Kafka em Docker Compose gerindo os tópicos `telemetria-pocos` (4 partições) e `alertas-operacionais` (1 partição), contrato de dados JSON e validação de resiliência com checkpoint (`src/streaming/validacao_spark_p2.py`).
3. **Módulo 3 — Spark Streaming, ML, Persistência e Validação de Saídas (Pessoa 3):** Pré-processamento e normalização Z-Score online com parâmetros do Trabalho 2, modelo `pyspark.ml` (`KMeans` + `RandomForestClassifier`), agregações/médias por poço, gravação em **Parquet/Consola** e extração das métricas reais de execução.
4. **Módulo 4 — Motor de Regras de Alertas, Benchmarks e Relatório Técnico (Pessoa 4):** Filtragem de anomalias (regras R1, R2, R3 + *cooldown*), testes de estresse (`50`, `100`, `500`, `1000 msg/s`), gráficos em `resultados/graficos/` e relatório em [`relatorio_tecnico.md`](relatorio_tecnico.md).

---

## Módulo 1: Produtor Kafka (Ingestão de Dados)

O módulo produtor (`src/produtor/produtor_3w.py`) simula a telemetria em tempo real dos sensores dos poços de petróleo.

### Características do Produtor:
- **Varredura Recursiva:** Navega automaticamente por todas as subpastas de classes de anomalia (`0` a `9`).
- **Tratamento de Nulos:** Conversão automática de valores `NaN` para `None` (garantindo JSON válido).
- **Enriquecimento de Metadados:** Adiciona `timestamp_envio`, `poco_id` (ex: `WELL-00001`), `arquivo_origem` e `evento_classe` a cada mensagem.
- **Taxa de Transferência Configurável:** Suporta ajuste de delay entre envios e transmissão multi-poço.

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

## Módulo 3: Spark Streaming, Pipeline de ML, Persistência e Métricas (Pessoa 3)

O Módulo 3 realiza o consumo em tempo real do tópico Kafka `telemetria-pocos` via **PySpark Structured Streaming**, aplica a **normalização Z-Score online** com os parâmetros do Trabalho 2, executa o pipeline **`pyspark.ml`**, calcula **agregações e médias por poço** e grava as saídas validadas em **Parquet**, **Consola** e **Kafka**.

### Estrutura de Arquivos do Módulo 3
- `configs/parametros_estatisticos_t2.json`: Parâmetros estatísticos de referência ($\mu$, $\sigma$, mediana para imputação de nulos, mínimo, máximo e catálogo de falhas 0–9 e 101–109 do Dataset 3W).
- `src/streaming/gerar_parametros_t2.py`: Utilitário em lote (padrão *streaming/batching* do Trabalho 2) para recalcular incrementalmente as estatísticas de `data/raw/`.
- `src/streaming/preprocessamento.py`:
  - **Parser Unificado (`parsear_stream_kafka`):** Compatível com o produtor da Pessoa 1 (incluindo `evento_classe` e aliases) e com o esquema aninhado `sensores` da Pessoa 2.
  - **Normalização Online (`aplicar_preprocessamento_online`):** Imputação pela mediana histórica, Z-Score adaptativo para os 8 sensores do 3W e cálculo de `z_score_medio`, `z_score_max`, desvios $> 2\sigma$ / $> 3\sigma$ e diferenciais termohidráulicos.
- `src/ml/treinar_modelo.py`:
  - Treina, avalia (split 80/20 antes do `fit`, sem *data leakage*) e persiste o `pyspark.ml.Pipeline` (`Imputer` → `VectorAssembler` → `StandardScaler` → `KMeans(k=4)` → `RandomForestClassifier`) em `models/pipeline_ml_3w`, exportando `models/metricas_modelo.json` (**97,55% de acurácia e F1-Score**).
- `src/streaming/consumidor_spark_ml.py`:
  - Assina `telemetria-pocos`, executa o pré-processamento e a inferência ML em cada micro-batch, calcula agregações/médias por poço (`calcular_agregacoes_por_poco`), persiste as saídas em **Parquet** (`resultados/parquet/eventos_processados`, `agregacoes_pocos` e `alertas_anomalias`), exibe o resumo na **Consola** e publica anomalias no tópico `alertas-operacionais` (`--publicar-alertas`).
- `src/streaming/teste_pipeline_p3.py`: Validação autônoma fim a fim (Parser + ML + Agregações + gravação e releitura em Parquet) sem depender do Kafka.
- `src/streaming/validar_saidas_p3.py`: Auditoria das camadas Parquet/CSV e extração automática dos números reais em `resultados/resumo_metricas_p3.json`.

### Resumo dos Números Reais Validados (Execuções de 50 a 1.000 msg/s)
- **198.000 eventos processados** em **219 micro-batches** (0 mensagens perdidas)
- **20 poços monitorados** com **3.971 anomalias detectadas** pelo ML filtradas em **2.092 alertas** (**-47,3%** de ruído)
- **Latência média estável de ~3,3 s** entre 50 e 500 msg/s (~1,04 s/micro-batch; ~6–15 ms no Kafka; memória Spark estável em ~930–1.049 MB)
- **Acurácia / F1-Score do modelo:** **97,55%** (0 confusões entre *Normal* e *Anomalia Pontual Crítica*)

### Como Executar o Módulo 3

1. **(Opcional) Treinar / Atualizar o Modelo `pyspark.ml` Offline:**
   ```bash
   python -m src.ml.treinar_modelo
   ```

2. **Executar o Teste Integrado Autônomo (inclui validação de gravação e releitura Parquet):**
   ```bash
   python -m src.streaming.teste_pipeline_p3
   ```

3. **Iniciar o Consumidor Spark Structured Streaming (Consola + Parquet + Alertas Kafka):**
   ```bash
   python -m src.streaming.consumidor_spark_ml --bootstrap-servers localhost:9092 --topico-entrada telemetria-pocos --publicar-alertas
   ```

4. **Validar Saídas Persistidas e Extrair Números Reais da Execução:**
   ```bash
   python -m src.streaming.validar_saidas_p3
   ```

---

## Documentação e Apresentação
- Documentação detalhada do Módulo 3: [`DOCUMENTACAO_MODULO_3.md`](DOCUMENTACAO_MODULO_3.md)
- Relatório Técnico completo (com gráficos e benchmarks): [`relatorio_tecnico.md`](relatorio_tecnico.md)
- Slides e roteiro de fala da Pessoa 3: [`SLIDES_PESSOA_3.md`](SLIDES_PESSOA_3.md) e [`slides_pessoa_3.html`](slides_pessoa_3.html)
