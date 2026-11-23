# Manutenção Preditiva Petrolífera — Pipeline de Streaming com o Dataset 3W

Trabalho Final de **Sistemas Distribuídos** (UFF — Rio das Ostras).

O sistema simula a telemetria de poços de petróleo em tempo real a partir do **Dataset 3W da Petrobras**, transporta as leituras pelo **Apache Kafka** e as processa no **Apache Spark Structured Streaming**. No Spark, os dados são normalizados, classificados por um modelo de Machine Learning (`pyspark.ml`) e gravados como eventos, agregações por poço e alertas de anomalia.

---

## Arquitetura

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

## Pré-requisitos

- **Python 3.10+**
- **Java 17** (necessário para o PySpark)
- **Docker** e **Docker Compose**
- No Windows: `winutils.exe` e `hadoop.dll` configurados na variável `HADOOP_HOME`.

> Na primeira execução, o Spark baixa automaticamente o conector `spark-sql-kafka`, portanto é necessário acesso à internet.

---

## Como executar

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