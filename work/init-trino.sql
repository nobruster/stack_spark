-- ═══════════════════════════════════════════════════════════════════
--  TRINO — Inicialização dos catálogos Delta Lake
--
--  Nota sobre scheme:
--    Trino 435 com fs.native-s3.enabled=true usa s3:// (sem 'a')
--    O acesso MinIO é feito pelo filesystem nativo do Trino (sem Hadoop)
--
--  Este script é IDEMPOTENTE:
--    DROP TABLE IF EXISTS remove apenas a entrada no HMS (sem apagar dados S3)
--    Depois register_table recria a entrada apontando para os dados existentes.
--
--  Como executar (aguardar Trino inicializar ~30s):
--    docker exec -it trino-coordinator \
--      trino -f /etc/trino/init-trino.sql
--
--  Ou interativamente:
--    docker exec -it trino-coordinator trino
-- ═══════════════════════════════════════════════════════════════════


-- ─── 1. CAMADA OURO (Gold) — espaço principal do Trino ────────────

CREATE SCHEMA IF NOT EXISTS delta.ouro WITH (location = 's3://ouro/');

DROP TABLE IF EXISTS delta.ouro.fat_uf;
CALL delta.system.register_table(
    schema_name    => 'ouro',
    table_name     => 'fat_uf',
    table_location => 's3://ouro/pda/beneficios-emitidos/fat_uf'
);

DROP TABLE IF EXISTS delta.ouro.fat_especie;
CALL delta.system.register_table(
    schema_name    => 'ouro',
    table_name     => 'fat_especie',
    table_location => 's3://ouro/pda/beneficios-emitidos/fat_especie'
);

DROP TABLE IF EXISTS delta.ouro.fat_banco;
CALL delta.system.register_table(
    schema_name    => 'ouro',
    table_name     => 'fat_banco',
    table_location => 's3://ouro/pda/beneficios-emitidos/fat_banco'
);

DROP TABLE IF EXISTS delta.ouro.kpis_nacionais;
CALL delta.system.register_table(
    schema_name    => 'ouro',
    table_name     => 'kpis_nacionais',
    table_location => 's3://ouro/pda/beneficios-emitidos/kpis_nacionais'
);


-- ─── 2. CAMADA PRATA (Silver) — 41.5M linhas ─────────────────────

CREATE SCHEMA IF NOT EXISTS delta.prata WITH (location = 's3://prata/');

DROP TABLE IF EXISTS delta.prata.beneficios_emitidos;
CALL delta.system.register_table(
    schema_name    => 'prata',
    table_name     => 'beneficios_emitidos',
    table_location => 's3://prata/pda/beneficios-emitidos'
);


-- ─── 3. CAMADA BRONZE ────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS delta.bronze WITH (location = 's3://bronze/');

DROP TABLE IF EXISTS delta.bronze.beneficios_emitidos;
CALL delta.system.register_table(
    schema_name    => 'bronze',
    table_name     => 'beneficios_emitidos',
    table_location => 's3://bronze/pda/beneficios-emitidos'
);


-- ─── 4. Verificação ──────────────────────────────────────────────
SHOW SCHEMAS FROM delta;
SHOW TABLES FROM delta.ouro;

SELECT
    _ano_mes,
    total_beneficios,
    CAST(vl_total_brasil     AS DECIMAL(18,2)) AS vl_total_brasil,
    CAST(vl_medio_nacional   AS DECIMAL(10,2)) AS vl_medio,
    CAST(vl_mediano_nacional AS DECIMAL(10,2)) AS vl_mediano,
    pct_feminino
FROM delta.ouro.kpis_nacionais;
