"""
Camada de Normalização e Pré-processamento Online no PySpark (Pessoa 3).

Este módulo cumpre a Tarefa 2 da Pessoa 3:
"Estruturar a camada de normalização e pré-processamento online utilizando
os parâmetros estatísticos validados no Trabalho 2."

Principais responsabilidades:
1. Decodificar o payload JSON vindo do Kafka (compatível simultaneamente com o
   esquema plano da Pessoa 1 em `src/produtor/produtor_3w.py` e com o esquema
   aninhado `sensores` da Pessoa 2 em `src/setup_kafka.py`).
2. Tratar valores ausentes (NULL / NaN) em tempo real utilizando as medianas/médias
   históricas validadas no Trabalho 2 (`configs/parametros_estatisticos_t2.json`).
3. Aplicar normalização Z-Score online por sensor: z_i = (x_i - mu_i) / sigma_i.
4. Gerar atributos estatísticos multivariados em streaming (`z_score_medio`,
   `z_score_max`, contagem de sensores acima de 2 sigmas e 3 sigmas, e diferenciais
   termohidráulicos de pressão e temperatura) para alimentar o `pyspark.ml`.
"""

import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame  # pyrefly: ignore[missing-import]
from pyspark.sql import functions as F  # pyrefly: ignore[missing-import]
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType  # pyrefly: ignore[missing-import]

# Caminho padrão para os parâmetros estatísticos validados no Trabalho 2
CAMINHO_PARAMETROS_T2 = Path("configs/parametros_estatisticos_t2.json")

# Lista canônica dos 8 sensores físicos do Dataset 3W da Petrobras
SENSORES_CANONICOS: List[str] = [
    "P-PDG",       # Pressão no Permanent Downhole Gauge (fundo do poço)
    "P-TPT",       # Pressão no Temperature and Pressure Transducer
    "T-TPT",       # Temperatura no Temperature and Pressure Transducer
    "P-MON-CKP",   # Pressão a montante do Choke de Produção
    "T-JUS-CKP",   # Temperatura a jusante do Choke de Produção
    "P-JUS-CKGL",  # Pressão a jusante do Choke de Gás Lift
    "T-JUS-CKGL",  # Temperatura a jusante do Choke de Gás Lift
    "QGL",         # Vazão de Gás Lift
]

# Nomes das colunas normalizadas (Z-Score) geradas para cada sensor
COLUNAS_NORM_SENSORES: List[str] = [
    f"{s.replace('-', '_')}_norm" for s in SENSORES_CANONICOS
]

# Conjunto completo de 14 features de entrada do vetor do modelo pyspark.ml
COLUNAS_FEATURES_ML: List[str] = COLUNAS_NORM_SENSORES + [
    "z_score_medio",            # Média de |Z-Score| entre os sensores ativos no evento
    "z_score_max",              # Maior |Z-Score| registrado no evento
    "qtd_sensores_fora_2sigma", # Qtd. de sensores com |Z| >= 2.0 (indício de anomalia iminente)
    "qtd_sensores_fora_3sigma", # Qtd. de sensores com |Z| >= 3.0 (indício de anomalia crítica)
    "diferencial_pressao_norm", # Gradiente normalizado de pressão (P_PDG_norm - P_TPT_norm)
    "diferencial_temp_norm",    # Queda térmica normalizada no choke (T_TPT_norm - T_JUS_CKP_norm)
]


def carregar_parametros_estatisticos(caminho: Path = CAMINHO_PARAMETROS_T2) -> Dict[str, Any]:
    """
    Carrega o arquivo JSON com os parâmetros estatísticos validados no Trabalho 2
    (média, desvio padrão, mediana para imputação e catálogo de classes 3W).
    """
    if not caminho.exists():
        raise FileNotFoundError(
            f"Arquivo de parâmetros estatísticos não encontrado em: {caminho.resolve()}"
        )
    with open(caminho, "r", encoding="utf-8") as f:
        return json.load(f)


def obter_schema_telemetria() -> StructType:
    """
    Define o schema unificado para leitura de JSON no Spark Structured Streaming.
    Suporta simultaneamente:
      - O formato da Pessoa 1 (`produtor_3w.py`: sensores na raiz do JSON e aliases do README)
      - O formato da Pessoa 2 (`setup_kafka.py`: dicionário aninhado `sensores`)
    """
    return StructType(
        [
            # Metadados de envio e rastreabilidade temporal
            StructField("timestamp_envio", DoubleType(), True),
            StructField("timestamp_evento", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("arquivo_origem", StringType(), True),
            StructField("poco_id", StringType(), True),
            StructField("class", DoubleType(), True),
            # Sensores no nível raiz (formato Pessoa 1 - Parquet 3W original)
            StructField("P-PDG", DoubleType(), True),
            StructField("P-TPT", DoubleType(), True),
            StructField("T-TPT", DoubleType(), True),
            StructField("P-MON-CKP", DoubleType(), True),
            StructField("T-JUS-CKP", DoubleType(), True),
            StructField("P-JUS-CKGL", DoubleType(), True),
            StructField("T-JUS-CKGL", DoubleType(), True),
            StructField("QGL", DoubleType(), True),
            # Aliases reduzidos documentados no README da Pessoa 1
            StructField("P-TP", DoubleType(), True),
            StructField("T-TP", DoubleType(), True),
            StructField("P-MON-CK", DoubleType(), True),
            StructField("T-JSC", DoubleType(), True),
            # Dicionário aninhado de sensores (formato Pessoa 2 - setup_kafka.py)
            StructField("sensores", MapType(StringType(), DoubleType()), True),
        ]
    )


def _limpar_nan(coluna: F.Column) -> F.Column:
    """Converte valores NaN de ponto flutuante (IEEE 754) em NULL do Spark SQL."""
    return F.when(F.isnan(coluna), F.lit(None).cast(DoubleType())).otherwise(coluna)


def parsear_stream_kafka(df_kafka: DataFrame) -> DataFrame:
    """
    Converte o binário `value` do Kafka em colunas estruturadas, unificando os
    sensores do Dataset 3W (independentemente de virem no formato da Pessoa 1 ou
    da Pessoa 2) e enriquecendo com timestamps de recepção no Spark.
    """
    schema = obter_schema_telemetria()

    # Extrai metadados nativos do Kafka (partição, offset, timestamp) e faz o parse do JSON
    df_parsed = (
        df_kafka.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), schema).alias("payload"),
        )
        .select(
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "payload.*",
        )
    )

    # Mapeamento de aliases para consolidar automaticamente o formato da Pessoa 1 e da Pessoa 2
    mapa_aliases = {
        "P-PDG": ["P-PDG"],
        "P-TPT": ["P-TPT", "P-TP"],
        "T-TPT": ["T-TPT", "T-TP"],
        "P-MON-CKP": ["P-MON-CKP", "P-MON-CK"],
        "T-JUS-CKP": ["T-JUS-CKP", "T-JSC"],
        "P-JUS-CKGL": ["P-JUS-CKGL"],
        "T-JUS-CKGL": ["T-JUS-CKGL"],
        "QGL": ["QGL"],
    }

    # Para cada sensor canônico, busca primeiro nas colunas raiz e depois no mapa 'sensores'
    for sensor_canonico, aliases in mapa_aliases.items():
        candidatos = []
        for alias in aliases:
            candidatos.append(_limpar_nan(F.col(f"`{alias}`")))
            candidatos.append(_limpar_nan(F.col("sensores").getItem(alias)))
        df_parsed = df_parsed.withColumn(sensor_canonico, F.coalesce(*candidatos))

    # Garante preenchimento de poco_id a partir de arquivo_origem (ex: WELL-00001_...) caso não venha explícito
    df_parsed = df_parsed.withColumn(
        "poco_id",
        F.coalesce(
            F.col("poco_id"),
            F.when(
                F.col("arquivo_origem").isNotNull() & F.col("arquivo_origem").contains("_"),
                F.split(F.col("arquivo_origem"), "_").getItem(0),
            ).otherwise(F.lit("POCO-DESCONHECIDO")),
        ),
    )

    # Timestamp do evento e timestamp exato de entrada no motor Spark (em segundos com fração de ms)
    df_parsed = df_parsed.withColumn(
        "timestamp_evento_str",
        F.coalesce(
            F.col("timestamp_evento"),
            F.col("timestamp"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"),
        ),
    ).withColumn(
        "timestamp_recebimento_spark",
        F.unix_timestamp(F.current_timestamp()).cast(DoubleType())
        + (F.unix_millis(F.current_timestamp()) % 1000) / 1000.0,
    )

    # Converte o código de classe real do 3W (0 = Normal, 1..9 = Falha, 101..109 = Transiente/Iminente)
    df_parsed = df_parsed.withColumn(
        "classe_real",
        F.when(
            F.col("class").isNotNull() & ~F.isnan(F.col("class")),
            F.col("class").cast("int"),
        ).otherwise(F.lit(0)),
    )

    colunas_selecionadas = [
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "timestamp_envio",
        "timestamp_evento_str",
        "timestamp_recebimento_spark",
        "arquivo_origem",
        "poco_id",
        "classe_real",
    ] + SENSORES_CANONICOS

    return df_parsed.select(*colunas_selecionadas)


def aplicar_preprocessamento_online(
    df: DataFrame, parametros: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Aplica a camada de normalização Z-Score e pré-processamento online utilizando
    os parâmetros estatísticos validados no Trabalho 2.

    Etapas executadas por registro em streaming:
      1. Contabilização de sensores ativos (`sensores_ativos`) e taxa de nulos (`taxa_nulos_linha`).
      2. Imputação de leituras ausentes com a mediana histórica de cada sensor (`<sensor>_imp`).
      3. Normalização Z-Score online (`<sensor>_norm` = (x - media) / desvio_padrao), com
         detecção automática de escala (Pascal para Parquet 3W bruto vs. bar/kPa para testes).
      4. Extração de indicadores estatísticos multivariados (`z_score_medio`, `z_score_max`,
         `qtd_sensores_fora_2sigma`, `qtd_sensores_fora_3sigma`) e gradientes físicos.
    """
    if parametros is None:
        parametros = carregar_parametros_estatisticos()

    cfg_sensores = parametros["sensores"]
    limiar_iminente = float(parametros["metadata"].get("limiar_zscore_iminente", 2.0))
    limiar_critico = float(parametros["metadata"].get("limiar_zscore_critico", 3.0))

    df_proc = df

    expressoes_ativos = []
    expressoes_abs_z = []
    expressoes_fora_2sigma = []
    expressoes_fora_3sigma = []

    # Sensores de pressão que podem vir em Pascal (Parquet 3W) ou bar/kPa (scripts de teste)
    sensores_pressao = {"P-PDG", "P-TPT", "P-MON-CKP", "P-JUS-CKGL"}

    for sensor in SENSORES_CANONICOS:
        prefixo = sensor.replace("-", "_")
        col_sensor = F.col(f"`{sensor}`")
        info = cfg_sensores[sensor]

        media_pa = float(info["media"])
        desvio_pa = max(float(info["desvio_padrao"]), 1e-6)
        mediana_pa = float(info["mediana"])

        media_red = float(info.get("media_escala_reduzida", media_pa))
        desvio_red = max(float(info.get("desvio_escala_reduzida", desvio_pa)), 1e-6)

        # 1. Verifica se o sensor está ativo (não nulo) neste evento
        expressoes_ativos.append(F.when(col_sensor.isNotNull(), F.lit(1)).otherwise(F.lit(0)))

        # 2. Imputa leitura ausente pela mediana validada no Trabalho 2
        col_imp_name = f"{prefixo}_imp"
        df_proc = df_proc.withColumn(
            col_imp_name,
            F.coalesce(col_sensor, F.lit(mediana_pa)),
        )

        # 3. Normalização Z-Score online:
        # Quando o sensor é nulo, atribui Z-Score = 0.0 (equivalente à média da distribuição).
        if sensor in sensores_pressao:
            z_bruto = F.when(col_sensor.isNull(), F.lit(0.0)).otherwise(
                F.when(
                    F.abs(col_sensor) > F.lit(50000.0),
                    (col_sensor - F.lit(media_pa)) / F.lit(desvio_pa),
                ).otherwise((col_sensor - F.lit(media_red)) / F.lit(desvio_red))
            )
        else:
            z_bruto = F.when(col_sensor.isNull(), F.lit(0.0)).otherwise(
                (col_sensor - F.lit(media_pa)) / F.lit(desvio_pa)
            )

        # Clamping em [-10.0, +10.0] para evitar que ruídos espúrios de hardware distorçam o modelo
        col_norm_name = f"{prefixo}_norm"
        df_proc = df_proc.withColumn(
            col_norm_name,
            F.greatest(F.lit(-10.0), F.least(F.lit(10.0), z_bruto)),
        )

        # Acumula o módulo do Z-Score e flags de violação de 2σ (iminente) e 3σ (crítico)
        z_abs = F.abs(F.col(col_norm_name))
        expressoes_abs_z.append(z_abs)
        expressoes_fora_2sigma.append(
            F.when(z_abs >= F.lit(limiar_iminente), F.lit(1.0)).otherwise(F.lit(0.0))
        )
        expressoes_fora_3sigma.append(
            F.when(z_abs >= F.lit(limiar_critico), F.lit(1.0)).otherwise(F.lit(0.0))
        )

    # Soma total de sensores ativos e proporção de sensores nulos na linha
    soma_ativos = expressoes_ativos[0]
    for expr in expressoes_ativos[1:]:
        soma_ativos = soma_ativos + expr

    df_proc = df_proc.withColumn("sensores_ativos", soma_ativos).withColumn(
        "taxa_nulos_linha",
        (F.lit(len(SENSORES_CANONICOS)) - F.col("sensores_ativos"))
        / F.lit(float(len(SENSORES_CANONICOS))),
    )

    # Agregações estatísticas multivariadas por evento
    soma_abs_z = expressoes_abs_z[0]
    for expr in expressoes_abs_z[1:]:
        soma_abs_z = soma_abs_z + expr

    soma_2sigma = expressoes_fora_2sigma[0]
    for expr in expressoes_fora_2sigma[1:]:
        soma_2sigma = soma_2sigma + expr

    soma_3sigma = expressoes_fora_3sigma[0]
    for expr in expressoes_fora_3sigma[1:]:
        soma_3sigma = soma_3sigma + expr

    df_proc = (
        df_proc.withColumn(
            "z_score_medio",
            soma_abs_z / F.greatest(F.col("sensores_ativos").cast(DoubleType()), F.lit(1.0)),
        )
        .withColumn("z_score_max", F.greatest(*expressoes_abs_z))
        .withColumn("qtd_sensores_fora_2sigma", soma_2sigma)
        .withColumn("qtd_sensores_fora_3sigma", soma_3sigma)
        # Gradientes físicos: diferencial de pressão fundo-cabeça e queda térmica no choke
        .withColumn(
            "diferencial_pressao_norm",
            F.col("P_PDG_norm") - F.col("P_TPT_norm"),
        )
        .withColumn(
            "diferencial_temp_norm",
            F.col("T_TPT_norm") - F.col("T_JUS_CKP_norm"),
        )
    )

    return df_proc
