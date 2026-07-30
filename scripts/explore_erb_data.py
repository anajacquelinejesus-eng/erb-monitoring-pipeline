"""
Passo 2 - Exploração inicial dos dados de ERBs (ANATEL)
Objetivo: entender volume, qualidade e estrutura antes de desenhar o pipeline.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when

spark = (
    SparkSession.builder
    .appName("explore-erb-data")
    .config("spark.sql.shuffle.partitions", "8")  # reduzido p/ rodar bem local
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

CSV_PATH = "data/raw/estacoes_smp.csv"

df = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("encoding", "windows-1252")
    .csv(CSV_PATH)
)

# Renomeia colunas para nomes ASCII seguros (evita problemas de encoding
# em etapas futuras: Kafka, Postgres, Parquet, etc.)
COLUNAS_ASCII = [
    "prestadora",
    "cnpj",
    "numero_estacao",
    "tipo_estacao",
    "uf",
    "codigo_uf",
    "municipio",
    "codigo_municipio",
    "logradouro",
    "latitude",
    "longitude",
    "freq_inicial_mhz",
    "freq_final_mhz",
    "azimute",
    "emissao",
]
df = df.toDF(*COLUNAS_ASCII)

print("\n=== SCHEMA ===")
df.printSchema()

total = df.count()
print(f"\n=== TOTAL DE LINHAS: {total:,} ===")

print("\n=== AMOSTRA (5 linhas) ===")
df.show(5, truncate=False)

print("\n=== % de Latitude mascarada ('*') ===")
df.select(
    count(when(col("latitude") == "*", 1)).alias("mascaradas"),
    count(when(col("latitude") != "*", 1)).alias("com_valor"),
).show()

print("\n=== Estações distintas (numero_estacao) ===")
df.select(countDistinct("numero_estacao").alias("estacoes_distintas")).show()

print("\n=== Municípios distintos ===")
df.select(countDistinct("codigo_municipio").alias("municipios_distintos")).show()

print("\n=== Operadoras (prestadora) - top 15 ===")
df.groupBy("prestadora").count().orderBy(col("count").desc()).show(15, truncate=False)

print("\n=== Distribuição por UF ===")
df.groupBy("uf").count().orderBy(col("count").desc()).show(30)

spark.stop()
