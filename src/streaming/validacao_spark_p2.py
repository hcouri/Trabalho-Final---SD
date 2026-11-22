import os
import sys

# 1. Configurações de ambiente (devem vir ANTES de importar o pyspark)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["JDK_JAVA_OPTIONS"] = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 2. Contrato de Dados (Schema JSON idêntico ao emitido por src/produtor/produtor_3w.py)
SCHEMA_TELEMETRIA = StructType([
    StructField("P-PDG", DoubleType(), True),
    StructField("P-TPT", DoubleType(), True),
    StructField("T-TPT", DoubleType(), True),
    StructField("P-MON-CKP", DoubleType(), True),
    StructField("T-JUS-CKP", DoubleType(), True),
    StructField("P-JUS-CKGL", DoubleType(), True),
    StructField("T-JUS-CKGL", DoubleType(), True),
    StructField("QGL", DoubleType(), True),
    StructField("class", DoubleType(), True),

    # Metadados obrigatórios adicionados pelo produtor_3w.py
    StructField("timestamp_envio", DoubleType(), True),
    StructField("arquivo_origem", StringType(), True),
    StructField("poco_id", StringType(), True),
])

KAFKA_BROKER = "localhost:9092"
TOPICO_TELEMETRIA = "telemetria-pocos"
CHECKPOINT_DIR = "checkpoints/spark_validacao_p2"


def iniciar_streaming_resiliente():
    # Pacote compatível com PySpark 4.2.0 (Scala 2.13)
    spark = (
        SparkSession.builder
        .appName("Pessoa2-ValidacaoSchemaEResiliencia")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df_bruto = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPICO_TELEMETRIA)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df_estruturado = (
        df_bruto
        .select(
            col("partition").alias("kafka_particao"),
            col("offset").alias("kafka_offset"),
            from_json(col("value").cast("string"), SCHEMA_TELEMETRIA).alias("dados")
        )
        .select("kafka_particao", "kafka_offset", "dados.*")
    )

    df_monitoramento = df_estruturado.select(
        "kafka_particao",
        "kafka_offset",
        "poco_id",
        "timestamp_envio",
        "arquivo_origem",
        "P-PDG",
        "T-TPT",
        "class"
    )

    query = (
        df_monitoramento.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 10)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    print(f"Streaming iniciado com checkpoint em: {os.path.abspath(CHECKPOINT_DIR)}")
    query.awaitTermination()


if __name__ == "__main__":
    iniciar_streaming_resiliente()