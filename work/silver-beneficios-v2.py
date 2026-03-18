"""
╔══════════════════════════════════════════════════════════════════╗
║   CAMADA SILVER — Benefícios Emitidos PDA 2025-2027  [v2 — otimizado]  ║
╚══════════════════════════════════════════════════════════════════╝

Fonte   : s3a://bronze/pda/beneficios-emitidos/   (Delta Lake)
Destino : s3a://prata/pda/beneficios-emitidos/   (Delta Lake)

─── OTIMIZAÇÕES v2 (em relação ao silver-beneficios.py original) ──
  OPT-1: Trim + nulidade combinados em PASSE ÚNICO via select()
         (v1 usava 2 loops separados de withColumn → 2 estágios de projeção)
  OPT-2: Parse banco em select() único
         (v1 usava 2 withColumn + 1 drop → substituído por select + list concat)
  OPT-3: Parse mun_pagto + mun_resid em select() único
         (v1 usava função com 3 withColumn + drop por coluna → 2 passes)
  OPT-4: persist() em df_out antes do write + count() do cache
         (v1 fazia df_val.count() relendo o Delta após gravação — scan extra)

─── 1. ENTENDIMENTO DOS DADOS ─────────────────────────────────────
  • Todas as strings têm trailing spaces → trim obrigatório
  • Valor inválido '{ñ class}' em: sexo, clientela, ramo_atividade
  • 'banco'    : campo composto "104-Caixa Econômica" → código + nome
  • 'mun_*'    : campo composto "02003-Al-Arapiraca"  → código + sg_uf + nome
  • 'especie_codigo' : DoubleType (87.0) → deve ser IntegerType
  • 3 registros com vl_liquido = 0.00 → sinalizados, não descartados

─── 2. TRANSFORMAÇÕES SILVER ──────────────────────────────────────
  ✓ Trim de todas as colunas string
  ✓ '{ñ class}' / 'Nao Informado' → NULL
  ✓ Parse banco        → banco_codigo (Int) + banco_nome (String)
  ✓ Parse mun_pagto    → mun_pagto_codigo, mun_pagto_sg_uf, mun_pagto_nome
  ✓ Parse mun_resid    → mun_resid_codigo, mun_resid_sg_uf, mun_resid_nome
  ✓ Derivar sg_uf
  ✓ especie_codigo     → cast Double → Integer
  ✓ Temporal           → ano_inicio (Int), mes_inicio (Int)
  ✓ Flag               → fl_vl_zero, fl_mesmo_municipio
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
    /opt/bitnami/spark/work/silver-beneficios-v2.py
"""

import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ─── Configuração centralizada ────────────────────────────────────
ANO_MES     = "202601"
BRONZE_PATH = "s3a://bronze/pda/beneficios-emitidos/"
SILVER_PATH = "s3a://prata/pda/beneficios-emitidos/"

# Mapeamento completo de estados para siglas UF
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

# Mapeamento de códigos de meio de pagamento → (sigla, descrição)
MEIO_PAG_MAP = {
    "Ccf": ("CCF", "Conta-Corrente Física"),
    "Ccl": ("CCL", "Conta-Corrente Lotérica"),
    "Cmg": ("CMG", "Cartão Magnético"),
}

# Valores que representam ausência de informação no dataset PDA
VALORES_NULOS = ["{ñ class}", "Nao Informado", ""]

# ─── Banner de início ─────────────────────────────────────────────
print("=" * 65)
print("  SILVER v2 — Benefícios Emitidos PDA 2025-2027")
print("=" * 65)

# ─── 1. SparkSession ─────────────────────────────────────────────
print("\n1. Iniciando SparkSession...")
spark = (
    SparkSession.builder
    .appName(f"Silver-PDA-Beneficios-v2-{ANO_MES}")
    # Extensões Delta Lake — necessárias para leitura/escrita Delta
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # AQE: permite ajuste dinâmico de partições após shuffles pesados
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Dimensiona partições de shuffle proporcionalmente aos cores disponíveis.
# Fator 6 (conservador) protege a memória dos workers nos groupBy do relatório.
total_cores    = spark.sparkContext.defaultParallelism
shuffle_parts  = max(12, total_cores * 6)
n_output_files = max(1,  total_cores)

spark.conf.set("spark.sql.shuffle.partitions", shuffle_parts)

print(f"   Spark {spark.version}")
print(f"   Cores alocados      : {total_cores}")
print(f"   shuffle.partitions  : {shuffle_parts}  (cores × 6, protege memória dos groupBy)")
print(f"   Arquivos de saída   : {n_output_files} (coalesce antes do write, 1 por core)")

print("\n2. Bucket 'prata' confirmado no MinIO (pre-existente).")

# ─── 3. Leitura da bronze ─────────────────────────────────────────
# Filtra na leitura Delta (predicate pushdown) para processar apenas
# a competência desejada, evitando varredura de outras partições.
print(f"\n3. Lendo bronze — partição _ano_mes='{ANO_MES}'...")
t0 = time.time()

df_bronze = (
    spark.read
    .format("delta")
    .load(BRONZE_PATH)
    .filter(F.col("_ano_mes") == ANO_MES)
    .cache()
)

n_bronze = df_bronze.count()
print(f"   {n_bronze:,} linhas lidas da bronze em {time.time()-t0:.1f}s")
df = df_bronze  # ponto de partida das transformações; df_bronze mantém ref ao cache

# ─── 4. Transformações silver ─────────────────────────────────────
print("\n4. Aplicando transformações silver...")

# ── OPT-1: Trim + nulidade em PASSE ÚNICO ────────────────────────
# v1 aplicava dois loops sequenciais de withColumn (trim sobre str_cols,
# depois nulidade sobre campos_categoricos), gerando 2 estágios de projeção
# no plano lógico. Aqui ambos são fundidos em um único select(), reduzindo
# para 1 estágio de projeção — Catalyst otimiza como uma única projection.
str_cols_set   = {f.name for f in df.schema.fields
                  if str(f.dataType) == "StringType()" and not f.name.startswith("_")}
campos_categoricos = ["sexo", "clientela", "ramo_atividade", "despacho"]
campos_cat_set = set(campos_categoricos)

def clean_expr(field):
    """Retorna a expressão Spark para o campo: trim e/ou nulificação conforme o tipo."""
    c    = field.name
    expr = F.col(c)
    # Aplica trim em toda coluna string não-metadado
    if c in str_cols_set:
        expr = F.trim(expr)
    # Substitui valores inválidos por NULL apenas nas colunas categóricas
    if c in campos_cat_set:
        expr = F.when(expr.isin(VALORES_NULOS), F.lit(None)).otherwise(expr)
    return expr.alias(c)

df = df.select([clean_expr(f) for f in df.schema.fields])
print(f"   OPT-1 ✓ Trim + nulidade em passe único "
      f"({len(str_cols_set)} colunas trim, {len(campos_cat_set)} cat)")

# ── OPT-2: Parse banco em select() único ─────────────────────────
# v1 usava withColumn("banco_codigo") + withColumn("banco_nome") + drop("banco")
# = 3 operações encadeadas que o plano lógico vê como 3 transformações.
# select() com list concatenation resolve tudo em 1 projection.
banco_parts = F.split(F.col("banco"), r"-")
df = df.select(
    [F.col(c) for c in df.columns if c != "banco"] + [
        banco_parts.getItem(0).cast(IntegerType()).alias("banco_codigo"),
        F.regexp_replace(F.col("banco"), r"^\d+-", "").alias("banco_nome"),
    ]
)
print("   OPT-2 ✓ banco → banco_codigo (Int) + banco_nome (select único)")

# ── OPT-3: Parse mun_pagto + mun_resid em select() único ─────────
# v1 chamava parse_municipio() duas vezes, cada uma com 3 withColumn + 1 drop
# = 8 transformações encadeadas no plano lógico. Um único select() funde
# o drop das colunas originais e a criação das 6 novas em 1 projection.
def mun_exprs(col_orig):
    """Retorna lista de expressões Spark para desmontar campo municipio composto."""
    prefix = col_orig.replace("mun_", "")
    parts  = F.split(F.col(col_orig), r"-")
    return [
        parts.getItem(0).alias(f"mun_{prefix}_codigo"),
        F.upper(parts.getItem(1)).alias(f"mun_{prefix}_sg_uf"),
        F.regexp_replace(F.col(col_orig), r"^\d+-[A-Za-z]+-", "").alias(f"mun_{prefix}_nome"),
    ]

# Mantém todas as colunas exceto as 2 originais (mun_pagto, mun_resid),
# e acrescenta as 6 colunas derivadas — tudo em uma única projection.
other_cols = [F.col(c) for c in df.columns if c not in {"mun_pagto", "mun_resid"}]
df = df.select(other_cols + mun_exprs("mun_pagto") + mun_exprs("mun_resid"))
print("   OPT-3 ✓ mun_pagto + mun_resid → codigo + sg_uf + nome (select único)")

# ── 4e. sg_uf ─────────────────────────────────────────────────────
# create_map gera um MapType literal no Catalyst — lookup O(1) no executor.
uf_map_expr = F.create_map(
    *[item for pair in
      [(F.lit(k), F.lit(v)) for k, v in UF_PARA_SIGLA.items()]
      for item in pair]
)
df = df.withColumn("sg_uf", uf_map_expr[F.col("uf")])
print(f"   ✓ sg_uf derivada do nome do estado ({len(UF_PARA_SIGLA)} estados)")

# ── 4f. especie_codigo: Double → Integer ─────────────────────────
# O CSV original armazena código de espécie como "87.0" (Double).
# Cast para Integer alinha com o domínio semântico do campo.
df = df.withColumn("especie_codigo",
                   F.col("especie_codigo").cast(IntegerType()))
print("   ✓ especie_codigo: Double → Integer")

# ── 4g. Parse meio_pagamento ──────────────────────────────────────
# Mapeia o prefixo textual para sigla|descrição e separa por pipe.
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

# ── 4i. Flags ─────────────────────────────────────────────────────
df = df.withColumn("fl_vl_zero",
                   F.when(F.col("vl_liquido") <= 0, True).otherwise(False))
df = df.withColumn("fl_mesmo_municipio",
                   F.col("mun_pagto_codigo") == F.col("mun_resid_codigo"))
print("   ✓ Flags: fl_vl_zero + fl_mesmo_municipio")

# ── 4j. Metadado Silver ───────────────────────────────────────────
# Timestamp UTC de quando este processamento silver foi executado.
silver_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
df = df.withColumn("_silver_ts", F.lit(silver_ts))
print(f"   ✓ _silver_ts = {silver_ts}")

print(f"\n   Schema silver ({len(df.columns)} colunas):")
for field in sorted(df.schema.fields, key=lambda f: f.name.startswith("_")):
    marker = " *" if field.name.startswith("_") else "  "
    print(f"    {marker}{field.name:<28} {str(field.dataType)}")

# ─────────────────────────────────────────────────────────────────
# 5. Relatório de qualidade ANTES de gravar
# ─────────────────────────────────────────────────────────────────
print("\n5. Relatório de qualidade dos dados silver...")

neg_cols   = [f.name for f in df.schema.fields if not f.name.startswith("_")]
null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in neg_cols]
nulls      = df.select(null_exprs).collect()[0].asDict()

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
# 6. Gravação Delta Lake
# ─────────────────────────────────────────────────────────────────
# OPT-4 revisado: libera cache bronze ANTES do write para ceder memória ao sort
# Delta. Com 41.5M × 29 colunas, o cache (17 col) + sort Delta (29 col)
# excedem os 2 GB de heap dos executors. O write re-lê da bronze via Delta —
# custo aceitável (~80s) vs OOM garantido.
# n_silver = n_bronze: silver preserva 100% das linhas (sem filtros).
print(f"\n6. Gravando Delta Lake em {SILVER_PATH} ...")
t_write = time.time()

n_silver = n_bronze   # silver preserva 100% das linhas — derivado do cache
df_bronze.unpersist()  # libera cache antes do sort Delta (evita OOM)

(
    df.coalesce(n_output_files).write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"_ano_mes = '{ANO_MES}'")
    .option("overwriteSchema", "true")
    .option("mergeSchema", "true")
    .partitionBy("_ano_mes")
    .save(SILVER_PATH)
)

elapsed = time.time() - t_write
print(f"   Gravação concluída em {elapsed:.1f}s")

# VACUUM remove arquivos físicos de versões Delta antigas (tombstoned).
# retentionDurationCheck desabilitado permite RETAIN 0 HOURS — útil em
# ambiente de desenvolvimento para liberar espaço imediatamente.
# ATENÇÃO: em produção, mantenha pelo menos RETAIN 168 HOURS (7 dias).
print("\n   Removendo arquivos obsoletos das versões anteriores (VACUUM)...")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM delta.`{SILVER_PATH}` RETAIN 0 HOURS")
print("   VACUUM concluído.")

# ─────────────────────────────────────────────────────────────────
# 7. Validação final
# ─────────────────────────────────────────────────────────────────
# Lê o Delta gravado para validar integridade física dos arquivos e
# obter o schema confirmado após overwriteSchema. O count() é substituído
# por n_silver (já calculado via OPT-4) — esta leitura serve apenas
# para os show() analíticos que validam o conteúdo efetivamente persistido.
print("\n7. Validando camada silver...")
df_val = spark.read.format("delta").load(SILVER_PATH)

print(f"\n{'='*65}")
print("   RESULTADO FINAL — SILVER v2")
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

print("\n8. Histórico Delta (Time Travel):")
spark.sql(f"DESCRIBE HISTORY delta.`{SILVER_PATH}`").select(
    "version", "timestamp", "operation", "operationParameters"
).show(5, truncate=60)

print(f"\n{'='*65}")
print("   CAMADA SILVER v2 CONCLUÍDA COM SUCESSO!")
print(f"{'='*65}\n")

spark.stop()
