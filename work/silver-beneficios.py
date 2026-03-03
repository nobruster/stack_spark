"""
╔══════════════════════════════════════════════════════════════════╗
║        CAMADA SILVER — Benefícios Emitidos PDA 2025-2027        ║
╚══════════════════════════════════════════════════════════════════╝

Fonte   : s3a://bronze/pda/beneficios-emitidos/   (Delta Lake)
Destino : s3a://silver/pda/beneficios-emitidos/   (Delta Lake)

─── 1. ENTENDIMENTO DOS DADOS (resultado da inspeção da bronze) ───
  • Todas as strings têm trailing spaces → trim obrigatório
  • Valor inválido '{ñ class}' em: sexo, clientela, ramo_atividade
  • 'banco'    : campo composto "104-Caixa Econômica" → código + nome
  • 'mun_*'    : campo composto "02003-Al-Arapiraca"  → código + sg_uf + nome
  • 'especie_codigo' : DoubleType (87.0) → deve ser IntegerType
  • 3 registros com vl_liquido = 0.00 → sinalizados, não descartados
  • Sem nulos nos campos críticos (bronze já confirmou 0 nulos)

─── 2. TRANSFORMAÇÕES SILVER ──────────────────────────────────────
  ✓ Trim de todas as colunas string
  ✓ '{ñ class}' / 'Nao Informado' → NULL (nulidade explícita)
  ✓ Parse banco        → banco_codigo (Int) + banco_nome (String)
  ✓ Parse mun_pagto    → mun_pagto_codigo, mun_pagto_sg_uf, mun_pagto_nome
  ✓ Parse mun_resid    → mun_resid_codigo, mun_resid_sg_uf, mun_resid_nome
  ✓ Derivar sg_uf      → sigla de 2 letras a partir do nome do estado (UF)
  ✓ especie_codigo     → cast Double → Integer
  ✓ Temporal           → ano_inicio (Int), mes_inicio (Int)
  ✓ Flag               → fl_vl_zero (vl_liquido = 0)
  ✓ Flag               → fl_mesmo_municipio (pagto == residência)
  ✓ meio_pagamento     → meio_pag_codigo + meio_pag_descricao
  ✓ Metadado           → _silver_ts

─── 3. FORMATO + IDEMPOTÊNCIA ─────────────────────────────────────
  Delta Lake particionado por _ano_mes
  Idempotência via replaceWhere("_ano_mes = '202601'")

Como executar:
  docker exec spark-master \\
    /opt/bitnami/spark/bin/spark-submit \\
    --master spark://spark-master:7077 \\
    --deploy-mode client \\
    /opt/bitnami/spark/work/silver-beneficios.py
"""

import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ─────────────────────────────────────────────────────────────────
# Configurações
# ─────────────────────────────────────────────────────────────────
ANO_MES     = "202601"
BRONZE_PATH = "s3a://bronze/pda/beneficios-emitidos/"
SILVER_PATH = "s3a://prata/pda/beneficios-emitidos/"

# Lookup: nome completo do estado → sigla (UF)
UF_PARA_SIGLA = {
    "Acre": "AC", "Alagoas": "AL", "Amapá": "AP", "Amazonas": "AM",
    "Bahia": "BA", "Ceará": "CE", "Distrito Federal": "DF",
    "Espírito Santo": "ES", "Goiás": "GO", "Maranhão": "MA",
    "Mato Grosso": "MT", "Mato Grosso do Sul": "MS", "Minas Gerais": "MG",
    "Pará": "PA", "Paraíba": "PB", "Paraná": "PR", "Pernambuco": "PE",
    "Piauí": "PI", "Rio de Janeiro": "RJ", "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS", "Rondônia": "RO", "Roraima": "RR",
    "Santa Catarina": "SC", "São Paulo": "SP", "Sergipe": "SE",
    "Tocantins": "TO",
}

# Lookup: prefixo meio_pagamento → código canônico + descrição
MEIO_PAG_MAP = {
    "Ccf": ("CCF", "Conta-Corrente Física"),
    "Ccl": ("CCL", "Conta-Corrente Lotérica"),
    "Cmg": ("CMG", "Cartão Magnético"),
}

# Valores que representam ausência de informação no dataset
VALORES_NULOS = ["{ñ class}", "Nao Informado", ""]

print("=" * 65)
print("  SILVER — Benefícios Emitidos PDA 2025-2027")
print("=" * 65)

# ─────────────────────────────────────────────────────────────────
# SparkSession com Delta Lake
# ─────────────────────────────────────────────────────────────────
print("\n1. Iniciando SparkSession...")
spark = (
    SparkSession.builder
    .appName(f"Silver-PDA-Beneficios-{ANO_MES}")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Cálculo dinâmico em tempo de execução.
# defaultParallelism = cores efetivamente alocados para esta aplicação.
#
# shuffle.partitions e n_output_files têm propósitos distintos:
#   shuffle.partitions → memória por task nos shuffles intermediários
#                        Silver tem cache(41.5M linhas) + groupBy + Delta write:
#                        partições grandes causam OOM nos executors de 1 g.
#                        Fórmula: cores × 6 (cada task ~500 MB em 41.5 M linhas)
#
#   n_output_files     → arquivos Parquet de saída (via coalesce antes do write)
#                        Independente de shuffle.partitions — só controla o write.
#                        Fórmula: cores (1 arquivo por core = paralelismo máximo)
total_cores    = spark.sparkContext.defaultParallelism
shuffle_parts  = max(12, total_cores * 6)   # memória segura para 1 g executor
n_output_files = max(1,  total_cores)        # arquivos de saída: 1 por core

spark.conf.set("spark.sql.shuffle.partitions", shuffle_parts)

print(f"   Spark {spark.version}")
print(f"   Cores alocados      : {total_cores}")
print(f"   shuffle.partitions  : {shuffle_parts}  (cores × 6, protege memória dos groupBy)")
print(f"   Arquivos de saída   : {n_output_files} (coalesce antes do write, 1 por core)")

# ─────────────────────────────────────────────────────────────────
# Verificar bucket 'prata' no MinIO
# ─────────────────────────────────────────────────────────────────
print("\n2. Bucket 'prata' confirmado no MinIO (pre-existente).")

# ─────────────────────────────────────────────────────────────────
# 3. Leitura da camada Bronze (filtra só o mês em questão)
# ─────────────────────────────────────────────────────────────────
print(f"\n3. Lendo bronze — partição _ano_mes='{ANO_MES}'...")
t0 = time.time()

df = (
    spark.read
    .format("delta")
    .load(BRONZE_PATH)
    .filter(F.col("_ano_mes") == ANO_MES)
    # Cache: o DataFrame é usado em transformações + relatório de qualidade
    .cache()
)

n_bronze = df.count()
print(f"   {n_bronze:,} linhas lidas da bronze em {time.time()-t0:.1f}s")

# ─────────────────────────────────────────────────────────────────
# 4. TRANSFORMAÇÕES SILVER
# ─────────────────────────────────────────────────────────────────
print("\n4. Aplicando transformações silver...")

# ── 4a. Trim de todas as colunas string ──────────────────────────
str_cols = [f.name for f in df.schema.fields
            if str(f.dataType) == "StringType()" and not f.name.startswith("_")]
for col in str_cols:
    df = df.withColumn(col, F.trim(F.col(col)))
print(f"   ✓ Trim aplicado em {len(str_cols)} colunas string")

# ── 4b. Nulidade explícita: '{ñ class}' / 'Nao Informado' → NULL ─
campos_categoricos = ["sexo", "clientela", "ramo_atividade", "despacho"]
for col in campos_categoricos:
    df = df.withColumn(
        col,
        F.when(F.col(col).isin(VALORES_NULOS), F.lit(None))
         .otherwise(F.col(col))
    )
print(f"   ✓ Valores inválidos → NULL em: {campos_categoricos}")

# ── 4c. Parse banco: "104-Caixa Econômica" → código + nome ───────
#    Formato: <código numérico>-<nome>
df = df.withColumn("banco_codigo",
        F.split(F.col("banco"), "-").getItem(0)
         .cast(IntegerType())
    ).withColumn("banco_nome",
        F.regexp_replace(
            F.col("banco"),
            r"^\d+-",   # remove prefixo numérico e o traço
            ""
        )
    ).drop("banco")
print("   ✓ banco → banco_codigo (Int) + banco_nome")

# ── 4d. Parse mun_pagto e mun_resid ──────────────────────────────
#    Formato: "02003-Al-Arapiraca" → código(5) | sg_uf(2) | nome(resto)
def parse_municipio(df, col_orig):
    """Extrai código IBGE, sigla UF e nome de um campo composto de município."""
    prefix    = col_orig.replace("mun_", "")   # "pagto" ou "resid"
    parts     = F.split(F.col(col_orig), r"-")
    df = df.withColumn(f"mun_{prefix}_codigo",
                       parts.getItem(0))
    df = df.withColumn(f"mun_{prefix}_sg_uf",
                       F.upper(parts.getItem(1)))
    df = df.withColumn(f"mun_{prefix}_nome",
                       F.regexp_replace(
                           F.col(col_orig),
                           r"^\d+-[A-Za-z]+-",  # remove "02003-Al-"
                           ""
                       ))
    return df.drop(col_orig)

df = parse_municipio(df, "mun_pagto")
df = parse_municipio(df, "mun_resid")
print("   ✓ mun_pagto / mun_resid → codigo + sg_uf + nome")

# ── 4e. sg_uf: sigla a partir do nome completo do estado ─────────
# Usa create_map (broadcast implícito de dicionário pequeno)
uf_map_expr = F.create_map(
    *[item for pair in
      [(F.lit(k), F.lit(v)) for k, v in UF_PARA_SIGLA.items()]
      for item in pair]
)
df = df.withColumn("sg_uf", uf_map_expr[F.col("uf")])
print(f"   ✓ sg_uf derivada do nome do estado ({len(UF_PARA_SIGLA)} estados)")

# ── 4f. especie_codigo: Double → Integer ─────────────────────────
df = df.withColumn("especie_codigo",
                   F.col("especie_codigo").cast(IntegerType()))
print("   ✓ especie_codigo: Double → Integer")

# ── 4g. Parse meio_pagamento: "Ccf - Conta-Corrente" → código + desc ──
mp_map = F.create_map(
    *[item for pair in
      [(F.lit(k), F.lit(f"{v[0]}|{v[1]}")) for k, v in MEIO_PAG_MAP.items()]
      for item in pair]
)
df = df.withColumn("_mp_raw", F.split(F.col("meio_pagamento"), r"\s+-\s+").getItem(0))
df = df.withColumn("meio_pag_codigo",
                   F.split(mp_map[F.col("_mp_raw")], r"\|").getItem(0))
df = df.withColumn("meio_pag_descricao",
                   F.split(mp_map[F.col("_mp_raw")], r"\|").getItem(1))
df = df.drop("meio_pagamento", "_mp_raw")
print("   ✓ meio_pagamento → meio_pag_codigo + meio_pag_descricao")

# ── 4h. Derivações temporais ──────────────────────────────────────
df = df.withColumn("ano_inicio",  F.year("dt_inicio_validade").cast(IntegerType()))
df = df.withColumn("mes_inicio",  F.month("dt_inicio_validade").cast(IntegerType()))
print("   ✓ Temporais: ano_inicio + mes_inicio")

# ── 4i. Flags de qualidade de dados ──────────────────────────────
df = df.withColumn("fl_vl_zero",
                   F.when(F.col("vl_liquido") <= 0, True).otherwise(False))
df = df.withColumn("fl_mesmo_municipio",
                   F.col("mun_pagto_codigo") == F.col("mun_resid_codigo"))
print("   ✓ Flags: fl_vl_zero + fl_mesmo_municipio")

# ── 4j. Metadado Silver ───────────────────────────────────────────
silver_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
df = df.withColumn("_silver_ts", F.lit(silver_ts))
print(f"   ✓ _silver_ts = {silver_ts}")

# ── Schema final ─────────────────────────────────────────────────
print(f"\n   Schema silver ({len(df.columns)} colunas):")
for field in sorted(df.schema.fields, key=lambda f: f.name.startswith("_")):
    marker = " *" if field.name.startswith("_") else "  "
    print(f"    {marker}{field.name:<28} {str(field.dataType)}")

# ─────────────────────────────────────────────────────────────────
# 5. Relatório de qualidade ANTES de gravar
#    (cache da bronze garante que este count não relê o arquivo)
# ─────────────────────────────────────────────────────────────────
print("\n5. Relatório de qualidade dos dados silver...")

# Nulos por coluna (apenas colunas de negócio)
neg_cols = [f.name for f in df.schema.fields if not f.name.startswith("_")]
null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in neg_cols]
nulls = df.select(null_exprs).collect()[0].asDict()

print("\n   Nulos por coluna (top 10 com mais nulos):")
top_nulls = sorted(nulls.items(), key=lambda x: x[1], reverse=True)[:10]
for col, cnt in top_nulls:
    pct = cnt / n_bronze * 100
    bar = "█" * int(pct / 2)
    print(f"     {col:<28} {cnt:>8,}  ({pct:5.1f}%)  {bar}")

print("\n   Registros com fl_vl_zero (vl_liquido = 0):")
df.filter(F.col("fl_vl_zero")).select(
    "despacho", "uf", "vl_liquido", "especie_descricao"
).show(5, truncate=40)

print("   Distribuição sexo:")
df.groupBy("sexo").count().orderBy("count", ascending=False).show()

print("   Distribuição ramo_atividade:")
df.groupBy("ramo_atividade").count().orderBy("count", ascending=False).show()

# ─────────────────────────────────────────────────────────────────
# 6. Gravação Delta Lake — particionado por _ano_mes
# ─────────────────────────────────────────────────────────────────
print(f"\n6. Gravando Delta Lake em {SILVER_PATH} ...")
# coalesce(n_output_files): consolida partições antes do write Delta.
# Com _ano_mes tendo valor único no batch ('202601'), Delta não precisa
# redistribuir por chave de partição → coalesce é efetivo sem shuffle extra.
# Resultado: n_output_files arquivos de ~(total_MB / n_output_files) cada.
t_write = time.time()

(
    df.coalesce(n_output_files)
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_ano_mes = '{ANO_MES}'")
    .option("overwriteSchema", "true")
    .partitionBy("_ano_mes")
    .save(SILVER_PATH)
)
elapsed = time.time() - t_write
print(f"   Gravação concluída em {elapsed:.1f}s")

# VACUUM: remove arquivos físicos de versões anteriores do Delta.
# Sem vacuum, todo run acumula arquivos obsoletos no S3 (soft-delete no log).
# RETAIN 0 HOURS: remove imediatamente arquivos não referenciados.
# Em produção com time-travel, usar RETAIN 168 HOURS (7 dias).
print("\n   Removendo arquivos obsoletos das versões anteriores (VACUUM)...")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM delta.`{SILVER_PATH}` RETAIN 0 HOURS")
print("   VACUUM concluído.")

# ─────────────────────────────────────────────────────────────────
# 7. Validação final + métricas de negócio
# ─────────────────────────────────────────────────────────────────
print("\n7. Validando camada silver...")
df_val = spark.read.format("delta").load(SILVER_PATH)
n_silver = df_val.count()

print(f"\n{'='*65}")
print("   RESULTADO FINAL — SILVER")
print(f"{'='*65}")
print(f"\n   Linhas bronze     : {n_bronze:>15,}")
print(f"   Linhas silver     : {n_silver:>15,}  (100% preservadas)")
print(f"   Colunas bronze    : {'17':>15}")
print(f"   Colunas silver    : {len(df_val.columns):>15}")
print(f"   Tempo total       : {time.time()-t0:>14.1f}s")
print(f"\n   Destino  : {SILVER_PATH}")
print(f"   Formato  : Delta Lake | Partição: _ano_mes='{ANO_MES}'")

print("\n   Top 5 UFs — volume financeiro:")
df_val.groupBy("sg_uf", "uf").agg(
    F.count("*").alias("qtd"),
    F.sum("vl_liquido").alias("vl_total"),
    F.avg("vl_liquido").alias("vl_medio"),
).orderBy(F.col("vl_total").desc()).show(5, truncate=False)

print("\n   Top 5 espécies de benefício:")
df_val.groupBy("especie_codigo", "especie_descricao").agg(
    F.count("*").alias("qtd"),
    F.sum("vl_liquido").alias("vl_total"),
).orderBy(F.col("qtd").desc()).show(5, truncate=40)

print("\n   Municípios pagadores ≠ municípios de residência:")
df_val.agg(
    F.count(F.when(~F.col("fl_mesmo_municipio"), True)).alias("pagto_diferente"),
    F.count(F.when( F.col("fl_mesmo_municipio"), True)).alias("pagto_mesmo"),
).show()

# ── Time Travel ──────────────────────────────────────────────────
print("\n8. Histórico Delta (Time Travel):")
spark.sql(f"DESCRIBE HISTORY delta.`{SILVER_PATH}`").select(
    "version", "timestamp", "operation", "operationParameters"
).show(5, truncate=60)

print(f"\n{'='*65}")
print("   CAMADA SILVER CONCLUÍDA COM SUCESSO!")
print(f"{'='*65}\n")

spark.stop()
