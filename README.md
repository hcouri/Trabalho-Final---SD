# Manutenção Preditiva Petrolífera — Pipeline de Streaming com o Dataset 3W

Projeto de Engenharia de Dados e Streaming em Tempo Real desenvolvido para monitorização, processamento, persistência e detecção de anomalias em poços de petróleo utilizando **Apache Kafka**, **Docker** e **Apache Spark**.
Trabalho Final de **Sistemas Distribuídos** (UFF — Rio das Ostras).

O sistema simula a telemetria de poços de petróleo em tempo real a partir do **Dataset 3W da Petrobras**, transporta as leituras pelo **Apache Kafka** e as processa no **Apache Spark Structured Streaming**. No Spark, os dados são normalizados, classificados por um modelo de Machine Learning (`pyspark.ml`) e gravados como eventos, agregações por poço e alertas de anomalia.

---

## Arquitetura

```
Dataset 3W (Parquet) -> Produtor Python -> Kafka [telemetria-pocos] -> Spark Streaming + ML -> Agregações/Parquet + Motor de Regras -> Kafka [alertas-operacionais]
```

1. **Módulo 1 — Produtor Kafka (Pessoa 1):** Leitura dos ficheiros Parquet do Dataset 3W (`data/raw/`), tratamento de nulos, enriquecimento de metadados (`poco_id`, `timestamp_envio`, `arquivo_origem`, `evento_classe`) e transmissão em JSON.
2. **Módulo 2 — Infraestrutura Kafka & Resiliência (Pessoa 2):** Cluster Kafka em Docker Compose gerindo os tópicos `telemetria-pocos` (4 partições) e `alertas-operacionais` (1 partição), contrato de dados JSON e validação de resiliência com checkpoint (`src/streaming/validacao_spark_p2.py`).
3. **Módulo 3 — Spark Streaming, ML, Persistência e Validação de Saídas (Pessoa 3):** Pré-processamento e normalização Z-Score online com parâmetros do Trabalho 2, modelo `pyspark.ml` (`KMeans` + `RandomForestClassifier`), agregações/médias por poço, gravação em **Parquet/Consola** e extração das métricas reais de execução.
4. **Módulo 4 — Motor de Regras de Alertas, Benchmarks e Relatório Técnico (Pessoa 4):** Filtragem de anomalias (regras R1, R2, R3 + *cooldown*), testes de estresse (`50`, `100`, `500`, `1000 msg/s`), gráficos em `resultados/graficos/` e relatório em [`relatorio_tecnico.md`](relatorio_tecnico.md).
![Arquitetura do sistema](docs/arquitetura.png)

| # | Componente | Arquivo principal | Função |
|---|---|---|---|
| 1 | **Dataset 3W** | `data/raw/` | Séries temporais em Parquet, organizadas em 10 classes (0 = normal, 1 a 9 = eventos indesejados). |
| 2 | **Produtor Python** | `src/produtor/produtor_3w.py` | Seleciona 1 arquivo por classe (leitura estratificada) e transmite os poços em paralelo via Round-Robin, a 1 leitura por segundo por poço (1 Hz). Acrescenta `poco_id`, `timestamp_envio`, `arquivo_origem` e `evento_classe` e envia em JSON. |
| 3 | **Apache Kafka** | `configs/docker-compose.yml`, `src/setup_kafka.py` | Broker único em Docker no modo KRaft (sem Zookeeper). Tópicos `telemetria-pocos` (4 partições) e `alertas-operacionais` (1 partição). |
| 4 | **Spark Structured Streaming** | `src/streaming/consumidor_spark_ml.py` | Micro-batches de 2 s com checkpoint. Faz o parse do JSON, imputação e Z-Score com os parâmetros do Trabalho 2, e inferência com `KMeans` + `RandomForestClassifier`. |
| 5 | **Saídas** | `resultados/parquet/`, `src/alertas/` | Console, Parquet (eventos, agregações por poço e alertas) e motor de regras (R1, R2, R3 + *cooldown*) que publica no tópico `alertas-operacionais`. |

### Por que é um sistema distribuído

- **Desacoplamento produtor/consumidor:** o produtor não conhece o Spark; os dois se comunicam somente pelo Kafka.
- **Paralelismo:** o tópico `telemetria-pocos` tem 4 partições, e o Spark usa `spark.sql.shuffle.partitions = 4` para ler e processar em paralelo.
- **Tolerância a falhas:** o Kafka retém as mensagens e o Spark grava seus offsets no checkpoint. Se o consumidor cair, ele retoma de onde parou, sem perder mensagens.

---

## Estrutura do repositório

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
configs/
  docker-compose.yml              # Kafka (KRaft) em Docker
  parametros_estatisticos_t2.json # média, desvio-padrão e mediana por sensor (Trabalho 2)
src/
  produtor/produtor_3w.py         # Produtor Kafka multi-poço (1 Hz, Round-Robin)
  setup_kafka.py                  # Criação dos tópicos e contrato de dados
  streaming/
    preprocessamento.py           # Schema JSON, imputação e normalização Z-Score online
    consumidor_spark_ml.py        # Consumidor Spark + ML + persistência Parquet
    validacao_spark_p2.py         # Teste de schema e resiliência (checkpoint)
    teste_pipeline_p3.py          # Validação do pipeline sem Kafka
    validar_saidas_p3.py          # Auditoria das saídas e extração de métricas
  ml/treinar_modelo.py            # Treino e avaliação do pipeline pyspark.ml
  alertas/                        # Motor de regras de alertas (R1, R2, R3 + cooldown)
  benchmarks/                     # Gerador de carga, latência, memória e gráficos
models/metricas_modelo.json       # Métricas do modelo treinado
resultados/                       # CSVs dos experimentos, gráficos e resumo de métricas
relatorio_tecnico.md              # Relatório técnico completo
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
## Pré-requisitos

- **Python 3.10+**
- **Java 17** (necessário para o PySpark)
- **Docker** e **Docker Compose**
- No Windows: `winutils.exe` e `hadoop.dll` configurados na variável `HADOOP_HOME`.

> Na primeira execução, o Spark baixa automaticamente o conector `spark-sql-kafka`, portanto é necessário acesso à internet.

---

## Como executar

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
Todos os comandos são executados a partir da **raiz do repositório**.

### 1. Preparar o ambiente

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Baixar o Dataset 3W

Baixe o dataset no repositório oficial da Petrobras ([github.com/petrobras/3W](https://github.com/petrobras/3W)) e copie as subpastas de classe (`0` a `9`) para `data/raw/`:

```
data/raw/
  0/  WELL-00001_20170201010207.parquet ...
  1/  ...
  9/  ...
```

> A pasta `data/raw/` está no `.gitignore` e não é versionada.

### 3. Subir o Kafka e criar os tópicos

```bash
docker compose -f configs/docker-compose.yml up -d
python src/setup_kafka.py
```

O `setup_kafka.py` deve rodar **antes** do produtor. Caso contrário, o Kafka cria o tópico `telemetria-pocos` automaticamente com apenas 1 partição.

### 4. Treinar o modelo (apenas na primeira vez)

```bash
python -m src.ml.treinar_modelo
```

O modelo é salvo em `models/pipeline_ml_3w/` e as métricas em `models/metricas_modelo.json`.

### 5. Iniciar o consumidor Spark (Terminal 1)

```bash
python -m src.streaming.consumidor_spark_ml --publicar-alertas
```

Opções úteis:

| Opção | Descrição |
|---|---|
| `--starting-offsets earliest` | Lê o tópico desde o início (o padrão é `latest`). |
| `--trigger "2 seconds"` | Intervalo dos micro-batches. |
| `--sem-parquet` | Exibe apenas na console, sem gravar Parquet. |
| `--publicar-alertas` | Publica as anomalias no tópico `alertas-operacionais`. |

### 6. Iniciar o produtor (Terminal 2)

```bash
python -m src.produtor.produtor_3w
```

A cada segundo, o produtor envia uma leitura de cada poço ativo. No Terminal 1, o Spark exibe por micro-batch o resumo por status (`NORMAL`, `ANOMALIA_IMINENTE`, `ANOMALIA_PONTUAL`), as agregações por poço e a latência ponta a ponta.

### 7. Validar as saídas

```bash
python -m src.streaming.validar_saidas_p3
```

O script audita as tabelas Parquet em `resultados/parquet/` (`eventos_processados`, `agregacoes_pocos`, `alertas_anomalias`) e consolida as métricas em `resultados/resumo_metricas_p3.json`.

### 8. Encerrar

```bash
docker compose -f configs/docker-compose.yml down       # adicione -v para apagar os dados do Kafka
```

---

## Teste de resiliência

O teste demonstra que nenhuma mensagem se perde quando o consumidor é interrompido:

1. Com o produtor em execução, interrompa o consumidor Spark (`Ctrl+C`).
2. Aguarde alguns segundos. Nesse intervalo o Kafka continua recebendo e acumulando as mensagens.
3. Execute novamente o mesmo comando do consumidor.

O Spark lê os offsets salvos no checkpoint (`checkpoints/`) e processa as mensagens acumuladas no primeiro micro-batch após o reinício. O script `src/streaming/validacao_spark_p2.py` faz a mesma demonstração de forma mais visual, exibindo a partição e o offset de cada mensagem: após o reinício, a sequência de offsets continua sem lacunas.

> Para reiniciar o processamento do zero, apague a pasta `checkpoints/`.

---

## Contrato de dados (tópico `telemetria-pocos`)

Cada mensagem é um JSON plano com as colunas originais do 3W e os metadados adicionados pelo produtor (valores ilustrativos):

```json
{
  "timestamp": "2017-02-01 01:02:07",
  "P-PDG": 0.0,
  "P-TPT": 10092924.0,
  "T-TPT": 119.0386,
  "P-MON-CKP": 1609381.0,
  "T-JUS-CKP": 84.58771,
  "P-JUS-CKGL": -300917.8,
  "T-JUS-CKGL": null,
  "QGL": 0.0,
  "class": 0,
  "timestamp_envio": 1763856000.123,
  "arquivo_origem": "WELL-00001_20170201010207.parquet",
  "poco_id": "WELL-00001",
  "evento_classe": "0"
}
```

Valores ausentes (`NaN`) são enviados como `null` e imputados no Spark com a mediana histórica do sensor. O schema do Spark (`preprocessamento.py`) também aceita o formato com os sensores aninhados em um campo `sensores`.

---

## Benchmarks

Os experimentos usam um gerador de carga sintética a taxas fixas (50, 100, 500 e 1000 mensagens/s). Cada rodada exige 4 terminais, iniciados nesta ordem:

```bash
python -m src.alertas.pipeline_alertas --taxa 100 --cooldown 0
python -m src.benchmarks.medir_latencia --taxa 100 --duracao 150
python -m src.benchmarks.monitor_memoria --taxa 100 --duracao 150
python -m src.benchmarks.gerador_carga --taxa 100 --duracao 120
```

Para gerar os gráficos em `resultados/graficos/`:

```bash
python -m src.benchmarks.gerar_graficos
```

### Principais resultados

| Métrica | Valor |
|---|---|
| Eventos processados (4 experimentos) | 198.000 em 219 micro-batches |
| Mensagens perdidas | 0 |
| Anomalias detectadas | 3.971 (2,01%) |
| Latência média ponta a ponta (até 500 msg/s) | ~3,3 s |
| Memória do Spark | ~1 GB, estável em todas as taxas |
| Acurácia / F1 do modelo | 97,55% / 0,9755 |

Em 1000 msg/s, a latência sobe para ~21,5 s porque o consumidor lê no máximo 2000 mensagens por micro-batch (`maxOffsetsPerTrigger`). O Kafka absorve o excedente e não há perda. A análise completa está em [`relatorio_tecnico.md`](relatorio_tecnico.md).

---

## Equipe e responsabilidades

| Integrante | Responsabilidade |
|---|---|
| Pessoa 1 | Produtor e infraestrutura Kafka |
| Pessoa 2 | Spark Streaming: contrato de dados e resiliência |
| Pessoa 3 | Persistência, regras de negócio e métricas |
| Pessoa 4 | Documentação, arquitetura e roteiro da apresentação |

## Documentação complementar

- [`relatorio_tecnico.md`](relatorio_tecnico.md): relatório técnico com os experimentos e as limitações.
- [`DOCUMENTACAO_MODULO_3.md`](DOCUMENTACAO_MODULO_3.md): detalhes do pipeline Spark, ML e persistência.
