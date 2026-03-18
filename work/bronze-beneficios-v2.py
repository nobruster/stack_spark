"""
╔══════════════════════════════════════════════════════════════════╗
║    CAMADA BRONZE — Benefícios Emitidos PDA 2025-2027 [v2]       ║
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

Otimizações aplicadas em relação à v1:
  [OPT-1] AQE ativado no builder (coalesce adaptativo de partições)
  [OPT-2] shuffle.partitions dinâmico baseado em total_cores (evita default 200)
  [OPT-3] n_raw capturado ANTES das transformações (evita re-scan do landing)
  [OPT-4] Loop withColumnRenamed + withColumns encadeados substituídos por
          SELECT ÚNICO com todas as expressões — reduz número de stages Spark
  [OPT-5] Validação em passe único via agg() — substitui 3 count() separados
          que geravam 3 full scans do Delta, por 1 único scan
  [OPT-6] coalesce(n_write_files) antes do write — controla número de arquivos
          de saída no Delta proporcional aos cores disponíveis

Como executar:
  docker exec spark-master \\
    /opt/bitnami/spark/bin/spark-submit \\
    --master spark://spark-master:7077 \\
    --deploy-mode client \\
    /opt/bitnami/spark/work/bronze-beneficios-v2.py
"""

import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# ---------------------------------------------------------------------------
# Configuração central — altere apenas estas variáveis para novos períodos
# ---------------------------------------------------------------------------
ANO_MES      = "202601"
LANDING_PATH = f"s3a://landing/pda/beneficios-emitidos/{ANO_MES}/"
BRONZE_PATH  = "s3a://bronze/pda/beneficios-emitidos/"
SOURCE_TAG   = f"landing/pda/beneficios-emitidos/{ANO_MES}"

# ---------------------------------------------------------------------------
# Inicialização
# ---------------------------------------------------------------------------
print("=" * 65)
print("  BRONZE — Benefícios Emitidos PDA 2025-2027  [v2 — otimizado]")
print("=" * 65)
print(f"\n1. Iniciando SparkSession com Delta Lake e AQE...")

# [OPT-1] AQE: permite ao Spark reotimizar o plano em tempo de execução,
# incluindo coalesce adaptativo de partições de shuffle — evita o problema
# de gerar centenas de partições pequenas após joins e aggregations.
spark = (
    SparkSession.builder
    .appName(f"Bronze-PDA-Beneficios-{ANO_MES}-v2")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"   Spark {spark.version} | Delta Lake pronto | AQE ativado")

# [OPT-2] shuffle.partitions dinâmico: o padrão 200 é inadequado tanto para
# clusters pequenos (gera centenas de arquivos minúsculos) quanto para
# datasets muito grandes (gera partições grandes demais).
# Usamos total_cores como base para calibrar automaticamente.
total_cores   = spark.sparkContext.defaultParallelism
shuffle_parts = max(4, total_cores * 2)
n_write_files = max(1, total_cores)
spark.conf.set("spark.sql.shuffle.partitions", shuffle_parts)
print(f"   Cores: {total_cores} | shuffle.partitions: {shuffle_parts} | arquivos saída: {n_write_files}")

# ---------------------------------------------------------------------------
# Verificação do bucket bronze no MinIO via Hadoop FileSystem API
# (sem dependência de boto3 — funciona com as credenciais do spark-defaults)
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Leitura da landing
# ---------------------------------------------------------------------------
print(f"\n3. Lendo da landing: {LANDING_PATH}")
t0 = time.time()

df_raw = spark.read.parquet(LANDING_PATH)

print(f"   Schema original ({len(df_raw.columns)} colunas):")
for field in df_raw.schema.fields:
    print(f"     {field.name:<25} {str(field.dataType)}")

# [OPT-3] Capturar n_raw ANTES de transformar o DataFrame.
# No v1, a validação chamava df_raw.count() após o write, forçando um
# segundo full scan do landing. Aqui contamos uma única vez e reutilizamos
# o valor na comparação final.
print(f"\n   Contando linhas da landing (será reutilizado na validação)...")
n_raw = df_raw.count()
print(f"   Linhas na landing: {n_raw:,}")

# ---------------------------------------------------------------------------
# Transformações mínimas — SELECT ÚNICO [OPT-4]
# ---------------------------------------------------------------------------
print(f"\n4. Aplicando transformações mínimas (SELECT único)...")

# [OPT-4] Em vez de um loop de withColumnRenamed() + withColumn() encadeados
# (que gera um novo estágio de projeção por chamada), consolidamos tudo em
# um único select(). O Catalyst Optimizer pode assim gerar um plano físico
# mais enxuto com uma única projeção, sem stages intermediários desnecessários.

ingestion_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Expressão de conversão monetária em passo único:
# "1.621,00" → remove separador de milhar (.) → substitui vírgula por ponto → Decimal
vl_expr = (
    F.regexp_replace(
        F.regexp_replace(F.col("Vl Líquido"), r"\.", ""),  # remove separador de milhar
        r",", "."                                           # vírgula decimal → ponto
    ).cast(DecimalType(12, 2))
)

df = df_raw.select(
    F.col("Despacho").alias("despacho"),
    F.col("`Sexo.`").alias("sexo"),
    F.col("Clientela").alias("clientela"),
    F.col("Tipo Benefício").alias("tipo_beneficio"),
    F.col("UF").alias("uf"),
    F.col("Meio pagamento").alias("meio_pagamento"),
    F.col("Banco").alias("banco"),
    F.col("Mun Pagto").alias("mun_pagto"),
    F.col("Mun Resid").alias("mun_resid"),
    vl_expr.alias("vl_liquido"),
    F.col("Ramo Atividade").alias("ramo_atividade"),
    # Cast direto no select — to_date já retorna DateType, sem cast adicional necessário
    F.to_date(F.col("Dt início validade"), "dd/MM/yyyy").alias("dt_inicio_validade"),
    F.col("Espécie12").alias("especie_codigo"),
    F.col("Espécie13").alias("especie_descricao"),
    # Metadados de rastreabilidade
    F.lit(ANO_MES).alias("_ano_mes"),
    F.lit(SOURCE_TAG).alias("_source_path"),
    F.lit(ingestion_ts).alias("_ingestion_ts"),
)

print(f"   Schema bronze ({len(df.columns)} colunas):")
for field in df.schema.fields:
    marker = " *" if field.name.startswith("_") else "  "
    print(f"    {marker}{field.name:<25} {str(field.dataType)}")

# ---------------------------------------------------------------------------
# Escrita Delta Lake com idempotência via replaceWhere
# ---------------------------------------------------------------------------
print(f"\n5. Gravando Delta Lake em {BRONZE_PATH} ...")
print(f"   Partição: _ano_mes = '{ANO_MES}'  |  modo: overwrite (replaceWhere)")
print(f"   Arquivos de saída por partição: {n_write_files} (coalesce)")

t_write = time.time()

# [OPT-6] coalesce(n_write_files) antes do write:
# Sem coalesce, o número de arquivos gerados no Delta é igual ao número de
# partições Spark do DataFrame (pode ser centenas). Com coalesce proporcional
# aos cores, garantimos arquivos de tamanho razoável sem shuffle adicional
# (coalesce é narrow transformation — não gera shuffle, ao contrário de repartition).
(
    df.coalesce(n_write_files)
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_ano_mes = '{ANO_MES}'")
    .option("overwriteSchema", "true")
    .option("mergeSchema", "true")
    .partitionBy("_ano_mes")
    .save(BRONZE_PATH)
)

elapsed_write = time.time() - t_write
print(f"   Gravação concluída em {elapsed_write:.1f}s")

# ---------------------------------------------------------------------------
# Validação — passe único via agg() [OPT-5]
# ---------------------------------------------------------------------------
print(f"\n6. Validando camada bronze...")
df_val = spark.read.format("delta").load(BRONZE_PATH)

# [OPT-5] No v1, a validação chamava df_val.count(), depois
# df_val.filter(...).count() para nulos de vl_liquido, e depois
# df_val.filter(...).count() para nulos de dt_inicio — totalizando 3 full
# scans do Delta. Aqui consolidamos tudo em um único agg() → 1 scan total.
val = df_val.agg(
    F.count("*").alias("total"),
    F.sum(F.col("vl_liquido").isNull().cast("int")).alias("null_vl"),
    F.sum(F.col("dt_inicio_validade").isNull().cast("int")).alias("null_dt"),
).collect()[0]

row_count = val["total"]
null_vl   = val["null_vl"]
null_dt   = val["null_dt"]

print(f"\n{'='*65}")
print("   RESULTADO FINAL — BRONZE")
print(f"{'='*65}")
print(f"\n   Linhas gravadas    : {row_count:>15,}")
# Reutiliza n_raw capturado antes das transformações — sem novo scan [OPT-3]
print(f"   Linhas totais src  : {n_raw:>15,}")
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

print(f"\n7. Histórico Delta (Time Travel):")
spark.sql(f"DESCRIBE HISTORY delta.`{BRONZE_PATH}`").select(
    "version", "timestamp", "operation", "operationParameters"
).show(5, truncate=60)

print(f"\n{'='*65}")
print("   CAMADA BRONZE CONCLUÍDA COM SUCESSO!  [v2 — otimizado]")
print(f"{'='*65}\n")

spark.stop()
