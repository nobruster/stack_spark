"""
╔══════════════════════════════════════════════════════════════════╗
║    CAMADA OURO — Benefícios Emitidos PDA 2025-2027 [v2 — otimizado]  ║
╚══════════════════════════════════════════════════════════════════╝

Fonte   : s3a://prata/pda/beneficios-emitidos/   (Delta Lake Silver)
Destino : s3a://ouro/pda/beneficios-emitidos/    (Delta Lake — 4 tabelas)

─── 1. ANÁLISE DO ARQUIVO DE ENTRADA ──────────────────────────────
  41.572.553 benefícios | R$ 78.5 bilhões | ticket médio R$ 1.888
  57 espécies | 21 bancos | 29 colunas | particionado por _ano_mes
  Mediana: R$ 1.621 | P90: R$ 3.324

─── 2. DEFINIÇÃO DA CAMADA OURO ───────────────────────────────────
  Objetivo : dados curados, pré-agregados, prontos para BI/dashboard
  Princípios:
    • Granularidade declarada (cada tabela tem um grain explícito)
    • Sem dados brutos — apenas métricas e dimensões de análise
    • Denormalizado: dimensões embutidas para leitura em 1 query
    • Validação de regras de negócio antes da escrita
    • Cobertura 100% do dado silver (sem filtros excludentes)

─── 3. TABELAS GERADAS ────────────────────────────────────────────
  fat_uf        grain: (uf × _ano_mes)         ~27 linhas/mês
  fat_especie   grain: (especie × _ano_mes)     ~57 linhas/mês
  fat_banco     grain: (banco × _ano_mes)       ~21 linhas/mês
  kpis_nacionais grain: (_ano_mes)              1 linha/mês

─── 4. PERFORMANCE [v2] ───────────────────────────────────────────
  • PERSIST(MEMORY_AND_DISK): silver lida uma vez, reutilizada 4x
  • OPT-1 — shuffle.partitions dinâmico + AQE:
      shuffle_parts = max(12, total_cores × 4); AQE + coalescePartitions
      evita partition skew sem fixar um número estático
  • OPT-2 — persist() em cada fat_* antes do write:
      fat_uf, fat_especie, fat_banco, kpis persistidos antes de gravar
      materializa cache com count(); write consome o DataFrame em memória
      elimina a releitura implícita que ocorre quando fat_uf.count() é
      chamado DEPOIS do write (Spark descarta o plano logico apos write)
  • OPT-3 — validação cruzada sem re-leituras Delta:
      usa DataFrames já em cache em vez de spark.read.format("delta").load()
      elimina 6 jobs extras de leitura Delta no bloco de validação
  • OPT-4 — relatório final usando DataFrames em memória:
      todos os spark.read.format("delta").load(GOLD_FAT_*) do relatório
      substituídos por fat_uf / fat_especie / fat_banco / kpis já em cache
      elimina 8+ releituras de Delta no bloco de exibição
  • Spark SQL via createOrReplaceTempView: Catalyst otimiza JOINs e AGGs
  • Window Functions para rankings e percentuais sem joins extras
  • replaceWhere por _ano_mes: idempotência granular por mês

Como executar:
  docker exec spark-master \\
    /opt/bitnami/spark/bin/spark-submit \\
    --master spark://spark-master:7077 \\
    --deploy-mode client \\
    /opt/bitnami/spark/work/gold-beneficios-v2.py
"""

import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import LongType, DecimalType
from pyspark import StorageLevel

# ─────────────────────────────────────────────────────────────────
# Configurações
# ─────────────────────────────────────────────────────────────────
ANO_MES     = "202601"
SILVER_PATH = "s3a://prata/pda/beneficios-emitidos/"
GOLD_BASE   = "s3a://ouro/pda/beneficios-emitidos"

GOLD_FAT_UF        = f"{GOLD_BASE}/fat_uf/"
GOLD_FAT_ESPECIE   = f"{GOLD_BASE}/fat_especie/"
GOLD_FAT_BANCO     = f"{GOLD_BASE}/fat_banco/"
GOLD_KPIS          = f"{GOLD_BASE}/kpis_nacionais/"

# Classificação de espécies em grupos de negócio
GRUPO_ESPECIE_SQL = """
    CASE
        WHEN especie_codigo IN (1,2,3,21,22,23,26,27,28,29,54,55,56,59)
            THEN 'Pensao'
        WHEN especie_codigo IN (4,5,6,7,8,30,32,33,34,37,38,40,41,42,43,44,45,46,47,48,49,51,57,58,72,79)
            THEN 'Aposentadoria'
        WHEN especie_codigo IN (10,13,16,18,25,31,36,60)
            THEN 'Auxilio'
        WHEN especie_codigo IN (11,12,87,88)
            THEN 'Amparo_BPC'
        ELSE 'Outros'
    END
"""

GOLD_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

print("=" * 65)
print("  OURO — Benefícios Emitidos PDA 2025-2027 [v2]")
print("=" * 65)

# ─────────────────────────────────────────────────────────────────
# SparkSession com Delta Lake
# OPT-1: shuffle.partitions é definido dinamicamente após getOrCreate()
#        com base em total_cores; AQE + coalescePartitions habilitados
#        para reduzir partições vazias após shuffles pesados (AGGs, JOINs)
# ─────────────────────────────────────────────────────────────────
print("\n1. Iniciando SparkSession...")
spark = (
    SparkSession.builder
    .appName(f"Gold-PDA-Beneficios-v2-{ANO_MES}")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # OPT-1: AQE permite que o Catalyst ajuste planos em tempo de execução
    #        (ex.: coalesce de partições vazias, broadcast de tabelas pequenas)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# OPT-1: shuffle.partitions dinâmico — fator 4 equilibra paralelismo e
#        overhead de shuffle para cargas de AGG/JOIN com ~40M linhas
total_cores   = spark.sparkContext.defaultParallelism
# fat_uf/fat_especie: PERCENTILE_APPROX + GROUP BY sobre 41.5M linhas exige
# partições menores; fator 24 → ~870K linhas/partição com 2 cores.
shuffle_parts = max(48, total_cores * 24)
spark.conf.set("spark.sql.shuffle.partitions", shuffle_parts)
print(f"   Spark {spark.version} | Delta pronto | shuffle.partitions={shuffle_parts} (cores × 24, AQE on)")

# ─────────────────────────────────────────────────────────────────
# 2. Leitura da Silver — PERSIST: será varrida 4x (uma por tabela)
# ─────────────────────────────────────────────────────────────────
print(f"\n2. Lendo silver (cache em memória — reutilizada 4x)...")
t0 = time.time()

df = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
    .filter(F.col("_ano_mes") == ANO_MES)
    .persist(StorageLevel.DISK_ONLY)  # DISK_ONLY: libera heap p/ AGGs; evita re-leitura MinIO
)
n_total = df.count()   # força a materialização do cache
print(f"   {n_total:,} linhas em cache | {time.time()-t0:.1f}s")

# Registra como view SQL para usar Spark SQL nas agregações
df.createOrReplaceTempView("silver")

# ─────────────────────────────────────────────────────────────────
# ──── TABELA 1: fat_uf ───────────────────────────────────────────
# Grain: UF × _ano_mes
# Negócio: comparativo entre estados — volume, valor, perfil, ranking
# ─────────────────────────────────────────────────────────────────
print("\n3. Construindo fat_uf (grain: UF × mês)...")

fat_uf_raw = spark.sql(f"""
    SELECT
        uf,
        sg_uf,
        _ano_mes,

        -- Volume
        COUNT(*)                                    AS qtd_beneficios,
        SUM(vl_liquido)                             AS vl_total,
        AVG(vl_liquido)                             AS vl_medio,
        PERCENTILE_APPROX(vl_liquido, 0.5, 100)    AS vl_mediano,
        MIN(vl_liquido)                             AS vl_minimo,
        MAX(vl_liquido)                             AS vl_maximo,

        -- Perfil demográfico
        ROUND(100.0 * SUM(CASE WHEN sexo = 'Feminino'  THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                    AS pct_feminino,
        ROUND(100.0 * SUM(CASE WHEN sexo = 'Masculino' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                    AS pct_masculino,
        ROUND(100.0 * SUM(CASE WHEN ramo_atividade = 'Rural' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                    AS pct_rural,

        -- Operacional
        ROUND(100.0 * SUM(CASE WHEN fl_mesmo_municipio = true THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                    AS pct_pagto_mesmo_municipio,
        COUNT(DISTINCT mun_resid_codigo)            AS qtd_municipios_atendidos,
        COUNT(DISTINCT banco_codigo)                AS qtd_bancos_utilizados,

        -- Tipo predominante de benefício
        {GRUPO_ESPECIE_SQL}                          AS grupo_especie_top,

        -- Metadado
        '{GOLD_TS}'                                 AS _gold_ts
    FROM silver
    GROUP BY uf, sg_uf, _ano_mes, {GRUPO_ESPECIE_SQL}
""")

# Window: ranking nacional e percentual de participação
w_nac = Window.partitionBy("_ano_mes")

fat_uf = (
    fat_uf_raw
    .withColumn("rank_qtd",
        F.dense_rank().over(w_nac.orderBy(F.col("qtd_beneficios").desc())))
    .withColumn("rank_vl_total",
        F.dense_rank().over(w_nac.orderBy(F.col("vl_total").desc())))
    .withColumn("pct_nacional_qtd",
        F.round(F.col("qtd_beneficios") * 100.0 /
                F.sum("qtd_beneficios").over(w_nac), 2))
    .withColumn("pct_nacional_valor",
        F.round(F.col("vl_total") * 100.0 /
                F.sum("vl_total").over(w_nac), 2))
)

# OPT-2: persist() ANTES do write
#   Sem persist: fat_uf.count() após write força re-leitura do Delta
#   (Spark descarta o plano lógico após a ação de write e recalcula a partir
#   da fonte; persist() fixa o DataFrame materializado em memória)
fat_uf = fat_uf.persist()
n_fat_uf = fat_uf.count()   # materializa cache; sem re-leitura de Delta

# ── Validação de negócio: somatório de participações deve ser ~100% ─
total_pct = fat_uf.agg(F.sum("pct_nacional_qtd")).collect()[0][0]
assert abs(float(total_pct) - 100.0) < 1.0, \
    f"FALHA: pct_nacional_qtd soma {total_pct}, esperado ~100"
print(f"   ✓ Validação: pct_nacional_qtd soma {total_pct:.2f}% (esperado ~100)")

(fat_uf.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"_ano_mes = '{ANO_MES}'")
    .option("overwriteSchema", "true")
    .option("mergeSchema", "true")
    .partitionBy("_ano_mes")
    .save(GOLD_FAT_UF))
print(f"   ✓ fat_uf gravada: {n_fat_uf} linhas → {GOLD_FAT_UF}")

# ─────────────────────────────────────────────────────────────────
# ──── TABELA 2: fat_especie ──────────────────────────────────────
# Grain: especie_codigo × _ano_mes
# Negócio: análise por tipo de benefício — custo, perfil, alcance geo
# ─────────────────────────────────────────────────────────────────
print("\n4. Construindo fat_especie (grain: espécie × mês)...")

fat_especie = spark.sql(f"""
    WITH base AS (
        SELECT
            especie_codigo,
            especie_descricao,
            {GRUPO_ESPECIE_SQL}   AS grupo_especie,
            _ano_mes,
            uf,
            sg_uf,
            vl_liquido,
            sexo,
            ramo_atividade,
            clientela,
            fl_mesmo_municipio,
            fl_vl_zero
        FROM silver
    ),
    -- CTE: UF dominante por espécie (maior qtd)
    uf_top AS (
        SELECT especie_codigo, sg_uf AS uf_top,
               ROW_NUMBER() OVER (PARTITION BY especie_codigo
                                  ORDER BY COUNT(*) DESC) AS rn
        FROM base
        GROUP BY especie_codigo, sg_uf
    )
    SELECT
        b.especie_codigo,
        b.especie_descricao,
        b.grupo_especie,
        b._ano_mes,

        COUNT(*)                                        AS qtd_beneficios,
        SUM(b.vl_liquido)                               AS vl_total,
        AVG(b.vl_liquido)                               AS vl_medio,
        PERCENTILE_APPROX(b.vl_liquido, 0.5, 100)      AS vl_mediano,
        MAX(b.vl_liquido)                               AS vl_maximo,

        -- Perfil
        ROUND(100.0 * SUM(CASE WHEN b.sexo = 'Feminino'  THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                        AS pct_feminino,
        ROUND(100.0 * SUM(CASE WHEN b.ramo_atividade = 'Rural' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                        AS pct_rural,
        ROUND(100.0 * SUM(CASE WHEN b.clientela = 'Urbano' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                        AS pct_urbano,
        ROUND(100.0 * SUM(CASE WHEN b.fl_mesmo_municipio THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                        AS pct_mesmo_municipio,

        -- Cobertura
        COUNT(DISTINCT b.uf)                            AS qtd_ufs_atendidas,
        ut.uf_top                                       AS uf_com_mais_beneficios,

        -- Qualidade
        SUM(CASE WHEN b.fl_vl_zero THEN 1 ELSE 0 END)  AS qtd_vl_zero,

        '{GOLD_TS}'                                     AS _gold_ts
    FROM base b
    LEFT JOIN uf_top ut ON b.especie_codigo = ut.especie_codigo AND ut.rn = 1
    GROUP BY b.especie_codigo, b.especie_descricao, b.grupo_especie, b._ano_mes, ut.uf_top
    ORDER BY qtd_beneficios DESC
""")

# Adiciona ranking dentro do grupo
w_grupo = Window.partitionBy("_ano_mes", "grupo_especie").orderBy(F.col("qtd_beneficios").desc())
fat_especie = fat_especie.withColumn("rank_no_grupo", F.dense_rank().over(w_grupo))

# OPT-2: persist() ANTES do write — mesmo motivo que fat_uf
fat_especie = fat_especie.persist()
n_fat_esp   = fat_especie.count()   # materializa cache

(fat_especie.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"_ano_mes = '{ANO_MES}'")
    .option("overwriteSchema", "true")
    .option("mergeSchema", "true")
    .partitionBy("_ano_mes")
    .save(GOLD_FAT_ESPECIE))
print(f"   ✓ fat_especie gravada: {n_fat_esp} linhas → {GOLD_FAT_ESPECIE}")

# ─────────────────────────────────────────────────────────────────
# ──── TABELA 3: fat_banco ────────────────────────────────────────
# Grain: banco_codigo × _ano_mes
# Negócio: market share bancário no pagamento de benefícios
# ─────────────────────────────────────────────────────────────────
print("\n5. Construindo fat_banco (grain: banco × mês)...")

fat_banco_raw = spark.sql(f"""
    SELECT
        banco_codigo,
        banco_nome,
        _ano_mes,
        COUNT(*)                                        AS qtd_beneficios,
        SUM(vl_liquido)                                 AS vl_total,
        AVG(vl_liquido)                                 AS vl_medio,
        COUNT(DISTINCT uf)                              AS qtd_ufs_atendidas,
        COUNT(DISTINCT mun_pagto_codigo)                AS qtd_municipios_pagadores,
        ROUND(100.0 * SUM(CASE WHEN meio_pag_codigo = 'CCF' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                        AS pct_conta_corrente,
        ROUND(100.0 * SUM(CASE WHEN meio_pag_codigo = 'CMG' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                        AS pct_cartao,
        '{GOLD_TS}'                                     AS _gold_ts
    FROM silver
    GROUP BY banco_codigo, banco_nome, _ano_mes
""")

w_banco = Window.partitionBy("_ano_mes")
fat_banco = (
    fat_banco_raw
    .withColumn("rank_qtd",
        F.dense_rank().over(w_banco.orderBy(F.col("qtd_beneficios").desc())))
    .withColumn("market_share_pct",
        F.round(F.col("qtd_beneficios") * 100.0 /
                F.sum("qtd_beneficios").over(w_banco), 2))
    .withColumn("market_share_valor_pct",
        F.round(F.col("vl_total") * 100.0 /
                F.sum("vl_total").over(w_banco), 2))
)

# OPT-2: persist() ANTES do write — mesmo motivo que fat_uf
fat_banco = fat_banco.persist()
n_fat_ban = fat_banco.count()   # materializa cache

(fat_banco.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"_ano_mes = '{ANO_MES}'")
    .option("overwriteSchema", "true")
    .option("mergeSchema", "true")
    .partitionBy("_ano_mes")
    .save(GOLD_FAT_BANCO))
print(f"   ✓ fat_banco gravada: {n_fat_ban} linhas → {GOLD_FAT_BANCO}")

# ─────────────────────────────────────────────────────────────────
# ──── TABELA 4: kpis_nacionais ───────────────────────────────────
# Grain: _ano_mes (1 linha por mês)
# Negócio: painel executivo — KPIs de alto nível para dashboard
#
# Estratégia anti-OOM:
#   PERCENTILE_APPROX com accuracy=10000 (default) causa heap overflow
#   nos workers (2.5GB limit) com 41.5M rows.
#   Solução: separar em 2 queries:
#     (a) métricas sem percentil — sem pressão de memória
#     (b) percentis com accuracy=100 (~100x menos memória, ±1% de precisão)
#   Depois JOIN por _ano_mes (resultado: 1 linha por mês).
# ─────────────────────────────────────────────────────────────────
print("\n6. Construindo kpis_nacionais (grain: 1 linha por mês)...")

# 6a. Métricas sem percentil (COUNT, SUM, AVG, MAX, COUNT DISTINCT)
kpis_main = spark.sql(f"""
    SELECT
        _ano_mes,

        -- Volume
        COUNT(*)                                          AS total_beneficios,
        COUNT(DISTINCT uf)                                AS total_ufs_atendidas,
        COUNT(DISTINCT mun_resid_codigo)                  AS total_municipios_atendidos,

        -- Financeiro (sem PERCENTILE_APPROX)
        SUM(vl_liquido)                                   AS vl_total_brasil,
        AVG(vl_liquido)                                   AS vl_medio_nacional,
        MAX(vl_liquido)                                   AS vl_maximo_nacional,

        -- Perfil
        ROUND(100.0 * SUM(CASE WHEN sexo = 'Feminino'   THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                          AS pct_feminino,
        ROUND(100.0 * SUM(CASE WHEN ramo_atividade = 'Rural' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                          AS pct_rural,
        ROUND(100.0 * SUM(CASE WHEN fl_mesmo_municipio THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                          AS pct_pagto_mesmo_municipio,

        -- Diversidade
        COUNT(DISTINCT especie_codigo)                    AS total_especies_ativas,
        COUNT(DISTINCT banco_codigo)                      AS total_bancos_pagadores,

        -- Qualidade
        SUM(CASE WHEN fl_vl_zero     THEN 1 ELSE 0 END)  AS qtd_vl_zero,
        SUM(CASE WHEN sexo IS NULL   THEN 1 ELSE 0 END)  AS qtd_sexo_nulo

    FROM silver
    GROUP BY _ano_mes
""")

# 6b. Percentis separados com accuracy=100 (precision ~1%)
#     accuracy=100 usa ~100x menos memória que o default (10000)
#     Para valores na faixa R$ 1.000–10.000, erro máx ≈ R$ 10–100 — aceitável
print("   6b. Calculando percentis com accuracy=100 (low-memory)...")
kpis_perc = spark.sql(f"""
    SELECT
        _ano_mes,
        PERCENTILE_APPROX(vl_liquido, 0.5, 100) AS vl_mediano_nacional,
        PERCENTILE_APPROX(vl_liquido, 0.9, 100) AS vl_p90_nacional
    FROM silver
    GROUP BY _ano_mes
""")

# 6c. Merge: JOIN por _ano_mes + adiciona _gold_ts
kpis = (
    kpis_main
    .join(kpis_perc, on="_ano_mes", how="inner")
    .withColumn("_gold_ts", F.lit(GOLD_TS))
)

# OPT-2: persist() ANTES do write — mesmo motivo que fat_uf
kpis = kpis.persist()
kpis.count()   # materializa cache (resultado é 1 linha; custo negligenciável)

(kpis.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"_ano_mes = '{ANO_MES}'")
    .option("overwriteSchema", "true")
    .option("mergeSchema", "true")
    .partitionBy("_ano_mes")
    .save(GOLD_KPIS))
print(f"   ✓ kpis_nacionais gravada: 1 linha(s) → {GOLD_KPIS}")

# ─────────────────────────────────────────────────────────────────
# 7. Relatório final — validação cruzada entre tabelas
#
# OPT-3: usa DataFrames já em cache em vez de spark.read.format("delta").load()
#   v1 executava 4 leituras separadas de Delta + 2 agg = 6 jobs extras
#   v2 consome os DataFrames já materializados em memória (0 I/O de Delta)
# ─────────────────────────────────────────────────────────────────
print(f"\n7. Validação cruzada das tabelas gold...")

# OPT-3: collect() sobre DataFrame em cache — sem I/O de MinIO
kpi_row     = kpis.collect()[0]

# OPT-3: agg sobre DataFrame em cache — sem I/O de MinIO
sum_qtd_esp = fat_especie.agg(F.sum("qtd_beneficios")).collect()[0][0]

# fat_uf tem grain uf×grupo_especie, então a soma pode ser maior que o total
# fat_especie tem grain especie, então deve bater com o total
assert abs(int(sum_qtd_esp) - int(kpi_row["total_beneficios"])) < 10, \
    f"FALHA: fat_especie qtd {sum_qtd_esp} ≠ kpi {kpi_row['total_beneficios']}"
print(f"   ✓ fat_especie qtd = kpi_nacionais total ({int(sum_qtd_esp):,})")

print(f"\n{'='*65}")
print("   RESULTADO FINAL — CAMADA OURO")
print(f"{'='*65}")
print(f"\n   {'Tabela':<22} {'Linhas':>8}  Descrição")
print(f"   {'-'*55}")
print(f"   {'fat_uf':<22} {n_fat_uf:>8}  Métricas por estado (grain: UF × mês)")
print(f"   {'fat_especie':<22} {n_fat_esp:>8}  Métricas por tipo de benefício")
print(f"   {'fat_banco':<22} {n_fat_ban:>8}  Market share bancário")
print(f"   {'kpis_nacionais':<22} {1:>8}  Painel executivo nacional")

print(f"\n   ── KPIs Nacionais ({ANO_MES}) ──────────────────────────")
print(f"   Total benefícios    : {int(kpi_row['total_beneficios']):>15,}")
print(f"   Municípios atendidos: {int(kpi_row['total_municipios_atendidos']):>15,}")
print(f"   Valor total         : R$ {float(kpi_row['vl_total_brasil']):>18,.2f}")
print(f"   Valor médio         : R$ {float(kpi_row['vl_medio_nacional']):>18,.2f}")
print(f"   Mediana             : R$ {float(kpi_row['vl_mediano_nacional']):>18,.2f}")
print(f"   P90                 : R$ {float(kpi_row['vl_p90_nacional']):>18,.2f}")
print(f"   % Feminino          :    {float(kpi_row['pct_feminino']):>16.2f}%")
print(f"   % Rural             :    {float(kpi_row['pct_rural']):>16.2f}%")
print(f"   % Paga fora domicílio:   {100 - float(kpi_row['pct_pagto_mesmo_municipio']):>15.2f}%")
print(f"   Espécies ativas     : {int(kpi_row['total_especies_ativas']):>15,}")
print(f"   Bancos pagadores    : {int(kpi_row['total_bancos_pagadores']):>15,}")

# OPT-4: relatório final usando DataFrames em memória (sem re-leituras Delta)
#   v1: spark.read.format("delta").load(GOLD_FAT_UF).filter(...)  → leitura do MinIO
#   v2: fat_uf.filter(...)                                         → DataFrame em cache

print(f"\n   ── Top 5 UFs por valor total ───────────────────────────")
(fat_uf
    .filter("rank_vl_total <= 5")
    .select("rank_vl_total", "sg_uf", "qtd_beneficios", "vl_total", "vl_medio",
            "pct_feminino", "pct_nacional_valor")
    .orderBy("rank_vl_total")
    .show(5, truncate=False))

print(f"\n   ── Top 5 Espécies por volume ────────────────────────────")
(fat_especie
    .select("especie_codigo", "especie_descricao", "grupo_especie",
            "qtd_beneficios", "vl_total", "vl_medio", "uf_com_mais_beneficios")
    .orderBy(F.col("qtd_beneficios").desc())
    .show(5, truncate=40))

print(f"\n   ── Market Share Bancário (Top 5) ────────────────────────")
(fat_banco
    .select("rank_qtd", "banco_codigo", "banco_nome",
            "qtd_beneficios", "market_share_pct", "market_share_valor_pct")
    .orderBy("rank_qtd")
    .show(5, truncate=False))

print(f"\n   ── Grupos de Espécie ────────────────────────────────────")
(fat_especie
    .groupBy("grupo_especie")
    .agg(
        F.sum("qtd_beneficios").alias("qtd"),
        F.sum("vl_total").alias("vl_total"),
    )
    .orderBy(F.col("qtd").desc())
    .show(truncate=False))

print(f"\n   Tempo total: {time.time()-t0:.1f}s")
print(f"\n{'='*65}")
print("   CAMADA OURO CONCLUÍDA COM SUCESSO!")
print(f"{'='*65}\n")

# OPT-4: libera todos os DataFrames persistidos ao final
#   Ordem: fat_* primeiro (menores), depois df silver (maior)
fat_uf.unpersist()
fat_especie.unpersist()
fat_banco.unpersist()
kpis.unpersist()
df.unpersist()

spark.stop()
