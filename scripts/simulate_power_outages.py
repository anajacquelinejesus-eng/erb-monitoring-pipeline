"""
Passo 4 - Simulador de alarmes de queda de energia
Cruza estações ERB (ANATEL) de Juiz de Fora/MG com dados reais de chuva
horária do INMET (evento real: enchentes de fev/2026) para gerar eventos
simulados, mas justificáveis, de queda de energia por antena.

Regra (documentada e ajustável):
  chuva < 20mm/h  -> 0.5% de chance de queda (falha de fundo, sem relação c/ clima)
  20-30mm/h       -> 15% de chance
  30-50mm/h       -> 40% de chance
  >= 50mm/h       -> 70% de chance
Duração da queda: base proporcional à severidade + variação aleatória.
"""

import os
import sys
import random
from datetime import timedelta

# Garante que o Spark use exatamente este mesmo interpretador Python
# (o do venv) tanto no processo principal quanto nos workers internos.
# Sem isso, no Windows, o Spark pode cair no atalho da Microsoft Store.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

random.seed(42)  # reprodutibilidade

spark = (
    SparkSession.builder
    .appName("simulate-power-outages")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

ERB_PATH = "data/raw/estacoes_smp.csv"
INMET_PATH = "data/raw/inmet_2026/INMET_SE_MG_A518_JUIZ DE FORA_01-01-2026_A_30-06-2026.CSV"
OUTPUT_PATH = "data/processed/eventos_energia_simulados"

COLUNAS_ERB_ASCII = [
    "prestadora", "cnpj", "numero_estacao", "tipo_estacao", "uf", "codigo_uf",
    "municipio", "codigo_municipio", "logradouro", "latitude", "longitude",
    "freq_inicial_mhz", "freq_final_mhz", "azimute", "emissao",
]

# ---------- 1. Carrega ERBs de Juiz de Fora ----------
erb_df = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("encoding", "windows-1252")
    .csv(ERB_PATH)
    .toDF(*COLUNAS_ERB_ASCII)
    .filter((F.col("uf") == "MG") & (F.upper(F.col("municipio")) == "JUIZ DE FORA"))
    .select("numero_estacao", "prestadora", "municipio", "uf", "codigo_municipio")
    .dropDuplicates(["numero_estacao"])  # 1 linha por estação física (ignora duplicidade por frequência)
)

total_erbs = erb_df.count()
print(f"\n=== ERBs distintas em Juiz de Fora/MG: {total_erbs} ===")
erb_df.show(10, truncate=False)

# ---------- 2. Carrega dados de chuva do INMET (Juiz de Fora) ----------
# Spark não tem uma opção nativa de "pular N linhas" no leitor de CSV,
# então removemos as 8 linhas de metadados com Python puro antes de
# entregar o arquivo pro Spark.
INMET_CLEAN_PATH = "data/processed/inmet_juizdefora_clean.csv"

with open(INMET_PATH, "r", encoding="windows-1252") as f_in:
    linhas = f_in.readlines()

with open(INMET_CLEAN_PATH, "w", encoding="utf-8") as f_out:
    f_out.writelines(linhas[8:])  # pula as 8 linhas de metadados, mantém o cabeçalho real

inmet_df = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .csv(INMET_CLEAN_PATH)
)

# Renomeia só as colunas que vamos usar (o arquivo tem ~19 colunas)
inmet_df = (
    inmet_df
    .select(
        F.col("Data").alias("data"),
        F.col("Hora UTC").alias("hora_utc"),
        F.col(inmet_df.columns[2]).alias("chuva_mm_raw"),  # 3ª coluna = PRECIPITAÇÃO TOTAL, HORÁRIO (mm)
    )
    # troca vírgula decimal por ponto e converte pra double
    .withColumn("chuva_mm", F.regexp_replace("chuva_mm_raw", ",", ".").cast(DoubleType()))
    .withColumn("chuva_mm", F.when(F.col("chuva_mm").isNull(), 0.0).otherwise(F.col("chuva_mm")))
    # monta timestamp real a partir de Data (yyyy/MM/dd) + Hora UTC (ex: "0000 UTC")
    .withColumn("hora_limpa", F.substring(F.col("hora_utc"), 1, 2))
    .withColumn(
        "timestamp",
        F.to_timestamp(
            F.concat_ws(" ", F.col("data"), F.col("hora_limpa")),
            "yyyy/MM/dd HH"
        )
    )
    .select("timestamp", "chuva_mm")
    .filter(F.col("timestamp").isNotNull())
)

print("\n=== Amostra de chuva horária (INMET Juiz de Fora) ===")
inmet_df.orderBy("timestamp").show(5)

# ---------- 3. Foca no período do evento real (22-24/02/2026) p/ manter volume controlável ----------
inmet_evento = inmet_df.filter(
    (F.col("timestamp") >= "2026-02-21 00:00:00") &
    (F.col("timestamp") <= "2026-02-25 00:00:00")
)
horas_evento = inmet_evento.count()
print(f"\n=== Horas no período do evento (21-25/02/2026): {horas_evento} ===")

# ---------- 4. Produto cartesiano ERB x hora (todas as combinações possíveis) ----------
combinacoes = erb_df.crossJoin(inmet_evento)
print(f"\n=== Combinações ERB x hora a avaliar: {combinacoes.count()} ===")

# ---------- 5. Aplica a regra de probabilidade via UDFs do Spark ----------
@F.udf(returnType=DoubleType())
def probabilidade_queda(chuva_mm):
    if chuva_mm is None:
        return 0.005
    if chuva_mm >= 50:
        return 0.70
    elif chuva_mm >= 30:
        return 0.40
    elif chuva_mm >= 20:
        return 0.15
    else:
        return 0.005

@F.udf(returnType=IntegerType())
def duracao_minutos_udf(chuva_mm):
    import random as rnd
    valor = chuva_mm if chuva_mm is not None else 0.0
    if valor >= 50:
        base = 180
    elif valor >= 30:
        base = 90
    elif valor >= 20:
        base = 45
    else:
        base = 20
    return base + rnd.randint(-10, 60)

combinacoes_avaliadas = (
    combinacoes
    .withColumn("prob_queda", probabilidade_queda(F.col("chuva_mm")))
    .withColumn("sorteio", F.rand(seed=42))
    .filter(F.col("sorteio") < F.col("prob_queda"))
    .withColumn("duracao_minutos", duracao_minutos_udf(F.col("chuva_mm")))
)

eventos_df = combinacoes_avaliadas.select(
    "numero_estacao",
    "prestadora",
    "municipio",
    "uf",
    F.col("timestamp").alias("timestamp_queda"),
    F.col("chuva_mm").alias("chuva_mm_no_momento"),
    "duracao_minutos",
)

total_eventos = eventos_df.count()
print(f"\n=== Eventos de queda de energia gerados: {total_eventos} ===")
eventos_df.orderBy("timestamp_queda").show(20, truncate=False)

# ---------- 6. Salva resultado ----------
(
    eventos_df
    .withColumn("timestamp_fim_estimado", F.col("timestamp_queda") + F.expr("INTERVAL 1 HOUR") * (F.col("duracao_minutos") / 60))
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(OUTPUT_PATH)
)
print(f"\n=== Salvo em: {OUTPUT_PATH} ===")

spark.stop()
