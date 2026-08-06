"""
Passo 7 - Consumidor Spark Structured Streaming -> Postgres
Lê os alarmes do Kafka, processa (igual ao Passo 6) e persiste o resultado
em duas tabelas do Postgres via foreachBatch:

  - eventos_energia: cada alarme individual (append)
  - alarmes_agregados_municipio: visão consolidada por município/hora
    (a cada micro-batch, substitui o conteúdo pela agregação mais atual,
    já que o output mode é "complete")
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "alarmes-energia-erb"
CHECKPOINT_DIR_EVENTOS = "data/checkpoints/eventos_postgres"
CHECKPOINT_DIR_AGREGADO = "data/checkpoints/agregado_postgres"

JDBC_URL = "jdbc:postgresql://localhost:5433/erb_monitoring"
JDBC_PROPS = {"user": "erb_admin", "password": "erb_pass", "driver": "org.postgresql.Driver"}
PG_CONN_PARAMS = dict(host="localhost", port=5433, dbname="erb_monitoring", user="erb_admin", password="erb_pass")

spark = (
    SparkSession.builder
    .appName("erb-power-outage-consumer-postgres")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
    )
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

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

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

eventos = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(F.from_json("json_str", schema_alarme).alias("dados"))
    .select("dados.*")
    .withColumn("timestamp_evento", F.to_timestamp("timestamp_evento"))
    .withColumn("timestamp_publicacao", F.to_timestamp("timestamp_publicacao"))
    .withColumn(
        "timestamp_fim_estimado",
        F.col("timestamp_evento") + F.expr("INTERVAL 1 MINUTE") * F.col("duracao_estimada_minutos")
    )
)


# ---------- Sink 1: eventos individuais -> tabela eventos_energia ----------
def escrever_eventos(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    print(f"[eventos] Batch {batch_id}: {batch_df.count()} linhas -> Postgres")
    (
        batch_df
        .select(
            "id_alarme", "numero_estacao", "prestadora", "municipio", "uf",
            "tipo_alarme", "timestamp_evento", "timestamp_fim_estimado",
            "chuva_mm_no_momento", "duracao_estimada_minutos", "timestamp_publicacao",
        )
        .write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "eventos_energia")
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .mode("append")
        .save()
    )


query_eventos = (
    eventos
    .writeStream
    .foreachBatch(escrever_eventos)
    .option("checkpointLocation", CHECKPOINT_DIR_EVENTOS)
    .trigger(processingTime="10 seconds")
    .start()
)

# ---------- Sink 2: agregação por município -> tabela alarmes_agregados_municipio ----------
eventos_com_watermark = eventos.withWatermark("timestamp_evento", "2 hours")

agregado_municipio = (
    eventos_com_watermark
    .groupBy(F.window("timestamp_evento", "1 hour").alias("janela"), "municipio", "uf")
    .agg(
        F.approx_count_distinct("numero_estacao").alias("erbs_afetadas"),
        F.count("id_alarme").alias("total_alarmes"),
        F.avg("duracao_estimada_minutos").alias("duracao_media_minutos"),
        F.max("chuva_mm_no_momento").alias("chuva_maxima_mm"),
    )
    .select(
        F.col("janela.start").alias("janela_inicio"),
        F.col("janela.end").alias("janela_fim"),
        "municipio", "uf", "erbs_afetadas", "total_alarmes",
        "duracao_media_minutos", "chuva_maxima_mm",
    )
)


def escrever_agregado(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    print(f"[agregado] Batch {batch_id}: {batch_df.count()} linhas -> Postgres (substitui tabela)")

    # Output mode "complete" -> cada batch já traz o resultado inteiro
    # acumulado. Truncamos antes de reinserir, pra tabela sempre refletir
    # o estado mais atual (sem duplicar nem acumular linhas obsoletas).
    with psycopg.connect(**PG_CONN_PARAMS, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE alarmes_agregados_municipio;")

    (
        batch_df
        .write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "alarmes_agregados_municipio")
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .mode("append")
        .save()
    )


query_agregado = (
    agregado_municipio
    .writeStream
    .outputMode("complete")
    .foreachBatch(escrever_agregado)
    .option("checkpointLocation", CHECKPOINT_DIR_AGREGADO)
    .trigger(processingTime="10 seconds")
    .start()
)

print("\n=== Streaming -> Postgres iniciado. Aguardando dados do Kafka... ===")
print("=== Pressione Ctrl+C para parar ===\n")

spark.streams.awaitAnyTermination()
