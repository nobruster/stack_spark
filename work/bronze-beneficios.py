"""
╔══════════════════════════════════════════════════════════════════╗
║        CAMADA BRONZE — Benefícios Emitidos PDA 2025-2027        ║
╚══════════════════════════════════════════════════════════════════╝

Fonte   : s3a://landing/pda/beneficios-emitidos/202601/   (Parquet raw)
Destino : s3a://bronze/pda/beneficios-emitidos/           (Delta Lake)

Transformações mínimas aplicadas (bronze = fidelidade ao dado original):
  ✓ Normalização de nomes de colunas → snake_case sem acentos/espaços
  ✓ Cast vl_liquido  : string "1.621,00" → Decimal(12,2)
  ✓ Cast dt_inicio   : string "30/01/2026" → Date
  ✓ Metadados de rastreabilidade: _ingestion_ts, _source_path, _ano_mes
  ✗ SEM limpeza de dados, SEM regras de negócio, SEM filtragem de nulos

Idempotência  : Delta replaceWhere "_ano_mes = '202601'"
                → re-executar substitui apenas a partição do mês, sem duplicar

Como executar:
  docker exec spark-master \\
    /opt/bitnami/spark/bin/spark-submit \\
    --master spark://spark-master:7077 \\
    --deploy-mode client \\
    /opt/bitnami/spark/work/bronze-beneficios.py
"""

import os
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType

# ─────────────────────────────────────────────────────────────────
# Configurações
# ─────────────────────────────────────────────────────────────────
ANO_MES      = "202601"
LANDING_PATH = f"s3a://landing/pda/beneficios-emitidos/{ANO_MES}/"
BRONZE_PATH  = "s3a://bronze/pda/beneficios-emitidos/"
SOURCE_TAG   = f"landing/pda/beneficios-emitidos/{ANO_MES}"

# Mapeamento: nome original (landing) → nome normalizado (bronze)
# Regra: lowercase, sem acentos, espaços → _, pontuação removida
COLUMN_MAP = {
    "Despacho"          : "despacho",
    "Sexo."             : "sexo",
    "Clientela"         : "clientela",
    "Tipo Benefício"    : "tipo_beneficio",
    "UF"                : "uf",
    "Meio pagamento"    : "meio_pagamento",
    "Banco"             : "banco",
    "Mun Pagto"         : "mun_pagto",
    "Mun Resid"         : "mun_resid",
    "Vl Líquido"        : "vl_liquido",
    "Ramo Atividade"    : "ramo_atividade",
    "Dt início validade": "dt_inicio_validade",
    "Espécie12"         : "especie_codigo",
    "Espécie13"         : "especie_descricao",
}

# ─────────────────────────────────────────────────────────────────
# SparkSession com Delta Lake
# (JARs já presentes em /opt/bitnami/spark/jars/ — sem download)
# ─────────────────────────────────────────────────────────────────
print("=" * 65)
print("  BRONZE — Benefícios Emitidos PDA 2025-2027")
print("=" * 65)
print(f"\n1. Iniciando SparkSession com Delta Lake...")

spark = (
    SparkSession.builder
    .appName(f"Bronze-PDA-Beneficios-{ANO_MES}")
    # Habilita extensões Delta (DDL, DML, time travel, MERGE)
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    # Registra o catálogo Delta como catálogo padrão do Spark
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"   Spark {spark.version} | Delta Lake pronto")

# ─────────────────────────────────────────────────────────────────
# Criar bucket 'bronze' no MinIO se não existir
# ─────────────────────────────────────────────────────────────────
print(f"\n2. Verificando bucket 'bronze' no MinIO...")
jvm         = spark.sparkContext._jvm
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
fs          = jvm.org.apache.hadoop.fs.FileSystem.get(
                  jvm.java.net.URI.create("s3a://bronze"), hadoop_conf)
bucket_path = jvm.org.apache.hadoop.fs.Path("s3a://bronze/")

if not fs.exists(bucket_path):
    fs.mkdirs(bucket_path)
    print("   Bucket 'bronze' criado.")
else:
    print("   Bucket 'bronze' já existe.")

# ─────────────────────────────────────────────────────────────────
# 3. Leitura da camada Landing (Parquet, all-string schema)
# ─────────────────────────────────────────────────────────────────
print(f"\n3. Lendo da landing: {LANDING_PATH}")
t0 = time.time()

df_raw = spark.read.parquet(LANDING_PATH)

print(f"   Schema original ({len(df_raw.columns)} colunas):")
for field in df_raw.schema.fields:
    print(f"     {field.name:<25} {str(field.dataType)}")

# ─────────────────────────────────────────────────────────────────
# 4. Transformações mínimas
# ─────────────────────────────────────────────────────────────────
print(f"\n4. Aplicando transformações mínimas...")

# 4a. Renomear colunas para snake_case sem acentos
df = df_raw
for col_orig, col_novo in COLUMN_MAP.items():
    if col_orig in df.columns:
        df = df.withColumnRenamed(col_orig, col_novo)

# 4b. Cast vl_liquido: "1.621,00" (formato BR) → Decimal(12,2)
#     Remove separador de milhar (.) e troca vírgula decimal por ponto
df = df.withColumn(
    "vl_liquido",
    F.regexp_replace(F.col("vl_liquido"), r"\.", "")           # remove "."
     .cast("string"),
).withColumn(
    "vl_liquido",
    F.regexp_replace(F.col("vl_liquido"), r",", ".")            # "," → "."
     .cast(DecimalType(12, 2)),                                 # cast decimal
)

# 4c. Cast dt_inicio_validade: "30/01/2026" → Date
df = df.withColumn(
    "dt_inicio_validade",
    F.to_date(F.col("dt_inicio_validade"), "dd/MM/yyyy").cast(DateType()),
)

# 4d. Metadados de rastreabilidade (nunca presentes nos dados originais)
ingestion_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
df = (
    df
    .withColumn("_ano_mes",      F.lit(ANO_MES))          # partição mensal
    .withColumn("_source_path",  F.lit(SOURCE_TAG))        # origem
    .withColumn("_ingestion_ts", F.lit(ingestion_ts))      # timestamp ingestão
)

print(f"   Schema bronze ({len(df.columns)} colunas):")
for field in df.schema.fields:
    marker = " *" if field.name.startswith("_") else "  "
    print(f"    {marker}{field.name:<25} {str(field.dataType)}")

# ─────────────────────────────────────────────────────────────────
# 5. Gravação Delta Lake — particionado por _ano_mes
#    Idempotência: replaceWhere substitui apenas o mês em questão,
#    deixando partições de outros meses intactas.
# ─────────────────────────────────────────────────────────────────
print(f"\n5. Gravando Delta Lake em {BRONZE_PATH} ...")
print(f"   Partição: _ano_mes = '{ANO_MES}'  |  modo: overwrite (replaceWhere)")

t_write = time.time()
(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_ano_mes = '{ANO_MES}'")   # idempotência por mês
    .partitionBy("_ano_mes")                              # otimiza consultas mensais
    .save(BRONZE_PATH)
)
elapsed_write = time.time() - t_write
print(f"   Gravação concluída em {elapsed_write:.1f}s")

# ─────────────────────────────────────────────────────────────────
# 6. Validação — leitura de volta + métricas
# ─────────────────────────────────────────────────────────────────
print(f"\n6. Validando camada bronze...")
df_val = spark.read.format("delta").load(BRONZE_PATH)

row_count = df_val.count()
null_vl   = df_val.filter(F.col("vl_liquido").isNull()).count()
null_dt   = df_val.filter(F.col("dt_inicio_validade").isNull()).count()

print(f"\n{'='*65}")
print("   RESULTADO FINAL — BRONZE")
print(f"{'='*65}")
print(f"\n   Linhas gravadas    : {row_count:>15,}")
print(f"   Linhas totais src  : {df_raw.count():>15,}")
print(f"   Nulos vl_liquido   : {null_vl:>15,}  ({null_vl/row_count*100:.2f}%)")
print(f"   Nulos dt_inicio    : {null_dt:>15,}  ({null_dt/row_count*100:.2f}%)")
print(f"   Tempo total        : {time.time()-t0:>14.1f}s")
print(f"\n   Destino  : {BRONZE_PATH}")
print(f"   Partição : _ano_mes = '{ANO_MES}'")
print(f"   Formato  : Delta Lake  (ACID | Time Travel | Schema Evolution)")

print(f"\n   Amostra — Top UFs:")
(
    df_val
    .groupBy("uf")
    .agg(
        F.count("*").alias("qtd_beneficios"),
        F.sum("vl_liquido").alias("vl_total"),
    )
    .orderBy(F.col("qtd_beneficios").desc())
    .show(10, truncate=False)
)

print(f"\n   Primeiras 5 linhas da camada bronze:")
df_val.select(
    "despacho", "sexo", "uf", "tipo_beneficio",
    "vl_liquido", "dt_inicio_validade",
    "especie_codigo", "especie_descricao",
    "_ano_mes", "_ingestion_ts"
).show(5, truncate=35)

# ─────────────────────────────────────────────────────────────────
# 7. Histórico Delta (Time Travel)
# ─────────────────────────────────────────────────────────────────
print(f"\n7. Histórico Delta (Time Travel):")
spark.sql(f"DESCRIBE HISTORY delta.`{BRONZE_PATH}`").select(
    "version", "timestamp", "operation", "operationParameters"
).show(5, truncate=60)

print(f"\n{'='*65}")
print("   CAMADA BRONZE CONCLUÍDA COM SUCESSO!")
print(f"{'='*65}\n")

spark.stop()
