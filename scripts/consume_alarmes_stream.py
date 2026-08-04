"""
Passo 6 - Consumidor Spark Structured Streaming
Lê os alarmes de queda de energia do Kafka em tempo real e gera duas visões:

  1. Eventos individuais processados (cada alarme, com horário estimado de fim)
  2. Agregação por município/janela de tempo (quantas ERBs afetadas, por operadora)

Por enquanto escreve no console para validação. No Passo 7, trocamos o sink
de saída pelo Postgres (via foreachBatch).
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "alarmes-energia-erb"
CHECKPOINT_DIR_EVENTOS = "data/checkpoints/eventos_individuais"
CHECKPOINT_DIR_AGREGADO = "data/checkpoints/agregado_municipio"

spark = (
    SparkSession.builder
    .appName("erb-power-outage-consumer")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------- 1. Schema do alarme (deve bater com o que o produtor envia) ----------
schema_alarme = StructType([
    StructField("id_alarme", StringType()),
    StructField("numero_estacao", StringType()),
    StructField("prestadora", StringType()),
    StructField("municipio", StringType()),
    StructField("uf", StringType()),
    StructField("tipo_alarme", StringType()),
    StructField("timestamp_evento", StringType()),
    StructField("chuva_mm_no_momento", DoubleType()),
    StructField("duracao_estimada_minutos", IntegerType()),
    StructField("timestamp_publicacao", StringType()),
])

# ---------- 2. Lê o stream bruto do Kafka ----------
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# ---------- 3. Faz o parse do JSON (value do Kafka vem em bytes) ----------
eventos = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(F.from_json("json_str", schema_alarme).alias("dados"))
    .select("dados.*")
    .withColumn("timestamp_evento", F.to_timestamp("timestamp_evento"))
    .withColumn(
        "timestamp_fim_estimado",
        F.col("timestamp_evento") + F.expr("INTERVAL 1 MINUTE") * F.col("duracao_estimada_minutos")
    )
)

# ---------- 4. Visão 1: eventos individuais (validação linha a linha) ----------
query_eventos = (
    eventos
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 10)
    .option("checkpointLocation", CHECKPOINT_DIR_EVENTOS)
    .trigger(processingTime="10 seconds")
    .start()
)

# ---------- 5. Visão 2: agregação por município + janela de 1 hora ----------
eventos_com_watermark = eventos.withWatermark("timestamp_evento", "2 hours")

agregado_municipio = (
    eventos_com_watermark
    .groupBy(
        F.window("timestamp_evento", "1 hour"),
        "municipio",
        "uf",
    )
    .agg(
        F.approx_count_distinct("numero_estacao").alias("erbs_afetadas"),
        F.count("id_alarme").alias("total_alarmes"),
        F.avg("duracao_estimada_minutos").alias("duracao_media_minutos"),
        F.max("chuva_mm_no_momento").alias("chuva_maxima_mm"),
    )
    .orderBy("window")
)

query_agregado = (
    agregado_municipio
    .writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .option("checkpointLocation", CHECKPOINT_DIR_AGREGADO)
    .trigger(processingTime="10 seconds")
    .start()
)

print("\n=== Streaming iniciado. Aguardando dados do Kafka... ===")
print("=== Pressione Ctrl+C para parar ===\n")

spark.streams.awaitAnyTermination()
