"""
Teste: Trino enxerga tabela Delta nova automaticamente?

Resposta curta: NÃO.
O Trino precisa que a tabela seja registrada no Hive Metastore (HMS)
via CALL delta.system.register_table(...) para enxergá-la.

O que este script faz:
  1. Lê fat_uf (já existe no ouro) via Spark
  2. Gera fat_regiao — agrupa fat_uf por região geográfica
  3. Grava em s3a://ouro/pda/beneficios-emitidos/fat_regiao/ como Delta
  4. Imprime os comandos Trino para registrar e consultar

Como executar:
  docker exec spark-master \\
    /opt/bitnami/spark/bin/spark-submit \\
    --master spark://spark-master:7077 \\
    --deploy-mode client \\
    /opt/bitnami/spark/work/teste-trino-ouro.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD_FAT_UF   = "s3a://ouro/pda/beneficios-emitidos/fat_uf/"
GOLD_FAT_REG  = "s3a://ouro/pda/beneficios-emitidos/fat_regiao/"

# Mapeamento UF → região geográfica do Brasil
REGIAO_SQL = """
    CASE
        WHEN sg_uf IN ('AM','PA','AC','RO','RR','AP','TO') THEN 'Norte'
        WHEN sg_uf IN ('MA','PI','CE','RN','PB','PE','AL','SE','BA') THEN 'Nordeste'
        WHEN sg_uf IN ('MG','ES','RJ','SP') THEN 'Sudeste'
        WHEN sg_uf IN ('PR','SC','RS') THEN 'Sul'
        WHEN sg_uf IN ('MT','MS','GO','DF') THEN 'Centro-Oeste'
        ELSE 'Desconhecido'
    END
"""

print("=" * 60)
print("  TESTE: Spark → MinIO → Trino (registro manual)")
print("=" * 60)

spark = (
    SparkSession.builder
    .appName("Teste-Trino-fat_regiao")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── 1. Lê fat_uf (já existe no ouro) ─────────────────────────────
print("\n1. Lendo fat_uf do ouro...")
fat_uf = spark.read.format("delta").load(GOLD_FAT_UF)
fat_uf.createOrReplaceTempView("fat_uf")
print(f"   {fat_uf.count()} linhas lidas de {GOLD_FAT_UF}")

# ── 2. Agrega por região ──────────────────────────────────────────
print("\n2. Gerando fat_regiao (grain: regiao × mês)...")
fat_regiao = spark.sql(f"""
    SELECT
        {REGIAO_SQL}              AS regiao,
        _ano_mes,
        COUNT(DISTINCT sg_uf)     AS qtd_ufs,
        SUM(qtd_beneficios)       AS qtd_beneficios,
        SUM(vl_total)             AS vl_total,
        AVG(vl_medio)             AS vl_medio_regiao,
        AVG(pct_feminino)         AS pct_feminino_medio,
        AVG(pct_rural)            AS pct_rural_medio
    FROM fat_uf
    GROUP BY {REGIAO_SQL}, _ano_mes
    ORDER BY qtd_beneficios DESC
""")

fat_regiao.show(truncate=False)

# ── 3. Grava como Delta no ouro ───────────────────────────────────
print(f"\n3. Gravando em {GOLD_FAT_REG}...")
(
    fat_regiao.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_FAT_REG)
)
print("   ✓ fat_regiao gravada com sucesso no MinIO (ouro)")

# ── 4. Instruções para o Trino ────────────────────────────────────
print("""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  PRÓXIMO PASSO — Verificar e registrar no Trino
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  1. TENTAR consultar antes do registro (deve falhar):

     docker exec -it trino-coordinator trino \\
       --execute "SELECT * FROM delta.ouro.fat_regiao"

  2. REGISTRAR a tabela no HMS:

     docker exec -it trino-coordinator trino \\
       --execute "CALL delta.system.register_table(
           schema_name    => 'ouro',
           table_name     => 'fat_regiao',
           table_location => 's3://ouro/pda/beneficios-emitidos/fat_regiao'
       )"

  3. CONSULTAR após o registro:

     docker exec -it trino-coordinator trino \\
       --execute "SELECT * FROM delta.ouro.fat_regiao ORDER BY qtd_beneficios DESC"

  4. ADICIONAR ao init-trino.sql para persistir nos próximos deploys:

     DROP TABLE IF EXISTS delta.ouro.fat_regiao;
     CALL delta.system.register_table(
         schema_name    => 'ouro',
         table_name     => 'fat_regiao',
         table_location => 's3://ouro/pda/beneficios-emitidos/fat_regiao'
     );
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")

spark.stop()
