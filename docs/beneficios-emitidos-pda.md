# Dataset: Benefícios Emitidos PDA 2025-2027

## 1. Identificação do Arquivo

| Atributo         | Valor |
|------------------|-------|
| **Nome**         | D.SDA.PDA.003.EMI.202601 |
| **Competência**  | Janeiro de 2026 (202601) |
| **Fonte**        | Portal de Dados Abertos do Governo Federal (PDA) |
| **Órgão**        | Secretaria de Desenvolvimento Social e Assistência (SDA) |
| **Formato raw**  | CSV compactado (.ZIP) com separador `;` e encoding Latin-1 |
| **Tamanho CSV**  | ~11 GB (descompactado) |
| **Registros**    | 41.572.553 linhas |
| **URL pública**  | `armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2025_2027/...` |

---

## 2. Características do Arquivo Raw (Landing)

### 2.1 Colunas originais (14)

| # | Nome original      | Tipo raw  | Observação |
|---|-------------------|-----------|------------|
| 1 | Despacho          | String    | Tipo de despacho do benefício |
| 2 | Sexo.             | String    | Ponto no nome é original do arquivo |
| 3 | Clientela         | String    | Perfil do segurado |
| 4 | Tipo Benefício    | String    | Normal / Acumulado |
| 5 | UF                | String    | Nome completo do estado (ex: "São Paulo") |
| 6 | Meio pagamento    | String    | Campo composto: prefixo + descrição |
| 7 | Banco             | String    | Campo composto: código + nome (ex: "104-Caixa Econômica") |
| 8 | Mun Pagto         | String    | Campo composto: código IBGE + sg_uf + nome |
| 9 | Mun Resid         | String    | Campo composto: código IBGE + sg_uf + nome |
| 10 | Vl Líquido       | String    | Formato BR: "1.621,00" (separador de milhar e vírgula decimal) |
| 11 | Ramo Atividade   | String    | Atividade do segurado |
| 12 | Dt início validade | String  | Formato "dd/MM/yyyy" |
| 13 | Espécie12        | String    | Código da espécie de benefício |
| 14 | Espécie13        | String    | Descrição da espécie de benefício |

> **Nota:** O dataset original tem duas colunas com nome "Espécie" — foram desambiguadas
> na landing como `Espécie12` (código) e `Espécie13` (descrição).

### 2.2 Problemas de qualidade identificados

| Problema | Campos afetados | Tratamento |
|----------|----------------|------------|
| Trailing spaces em todas as strings | Todos | Trim na camada Silver |
| Valor inválido `{ñ class}` | sexo, clientela, ramo_atividade | → NULL na Silver |
| Valor `Nao Informado` | despacho | → NULL na Silver |
| Campos compostos (código + nome) | banco, mun_pagto, mun_resid, meio_pagamento | Parse + split na Silver |
| UF como nome completo ("São Paulo") | uf | sg_uf derivada via lookup na Silver |
| Número do espécie como Double (87.0) | especie_codigo | Cast → Integer na Silver |
| 3 registros com vl_liquido = 0 | vl_liquido | Sinalizados com `fl_vl_zero`, não descartados |
| Nomes de colunas com acentos/espaços/ponto | Todos | Normalizado para snake_case na Bronze |

### 2.3 Métricas de negócio (competência 202601)

| Métrica | Valor |
|---------|-------|
| Total de benefícios | 41.572.553 |
| Valor total (R$) | ~78,5 bilhões |
| Ticket médio (R$) | 1.888 |
| Mediana (R$) | 1.621 |
| Percentil 90 (R$) | 3.324 |
| Espécies distintas | 57 |
| Bancos pagadores | 21 |
| UF com maior volume | São Paulo (9,0 M benefícios) |

---

## 3. Metodologia Medallion

A stack implementa a arquitetura **Medallion** (também chamada de Lakehouse) com quatro camadas progressivas de qualidade e granularidade. Cada camada tem responsabilidades exclusivas e bem delimitadas.

```
Fonte Pública (S3 AWS)
        │
        ▼  download + extração
┌───────────────────┐
│   LANDING ZONE    │  s3a://landing/pda/beneficios-emitidos/202601/
│   (Parquet raw)   │  Formato original sem alteração de tipos
└────────┬──────────┘
         │  bronze-beneficios.py
         ▼
┌───────────────────┐
│     BRONZE        │  s3a://bronze/pda/beneficios-emitidos/
│  (Delta Lake)     │  Tipos corretos, snake_case, metadados de rastreio
└────────┬──────────┘
         │  silver-beneficios.py
         ▼
┌───────────────────┐
│     SILVER        │  s3a://prata/pda/beneficios-emitidos/
│  (Delta Lake)     │  Dados limpos, campos parseados, flags de qualidade
└────────┬──────────┘
         │  gold-beneficios.py
         ▼
┌───────────────────┐
│      GOLD         │  s3a://ouro/pda/beneficios-emitidos/
│  (Delta Lake)     │  4 tabelas agregadas, prontas para BI/Trino
└───────────────────┘
```

---

## 4. Camadas em Detalhe

### 4.1 Landing Zone

**Script:** `work/landing-beneficios.py`
**Destino:** `s3a://landing/pda/beneficios-emitidos/202601/`
**Formato:** Parquet (sem inferência de tipo — tudo string)

**Responsabilidades:**
- Download do ZIP da fonte pública (com cache local para re-execuções)
- Detecção automática do separador CSV
- Leitura com `inferSchema=False` — preserva o dado exatamente como veio
- Desambiguação de colunas duplicadas (`Espécie12` / `Espécie13`)
- Escrita em Parquet no MinIO com `coalesce(n_cores)` para controle de arquivos

**O que NÃO faz:** nenhum cast, nenhuma limpeza, nenhuma regra de negócio.

**Saída:** 2 arquivos Parquet × ~185 MB cada (2 cores × 1 arquivo/core)

---

### 4.2 Bronze

**Script:** `work/bronze-beneficios.py`
**Fonte:** `s3a://landing/pda/beneficios-emitidos/202601/`
**Destino:** `s3a://bronze/pda/beneficios-emitidos/`
**Formato:** Delta Lake, particionado por `_ano_mes`

**Responsabilidades (transformações mínimas):**

| Transformação | Detalhe |
|---------------|---------|
| Renomear colunas | snake_case sem acentos/espaços/pontuação |
| Cast `vl_liquido` | String "1.621,00" → Decimal(12,2) (formato BR) |
| Cast `dt_inicio_validade` | String "30/01/2026" → Date |
| Metadados | `_ano_mes`, `_source_path`, `_ingestion_ts` |

**O que NÃO faz:** sem trim, sem limpeza de valores inválidos, sem parse de campos compostos.

**Idempotência:** `replaceWhere("_ano_mes = '202601'")` — re-execução substitui apenas o mês.

**Schema bronze (17 colunas):**

```
despacho            StringType
sexo                StringType
clientela           StringType
tipo_beneficio      StringType
uf                  StringType
meio_pagamento      StringType
banco               StringType
mun_pagto           StringType
mun_resid           StringType
vl_liquido          Decimal(12,2)
ramo_atividade      StringType
dt_inicio_validade  Date
especie_codigo      StringType
especie_descricao   StringType
_ano_mes            StringType    ← partição
_source_path        StringType    ← rastreabilidade
_ingestion_ts       StringType    ← timestamp de ingestão
```

---

### 4.3 Silver

**Script:** `work/silver-beneficios.py`
**Fonte:** `s3a://bronze/pda/beneficios-emitidos/`
**Destino:** `s3a://prata/pda/beneficios-emitidos/`
**Formato:** Delta Lake, particionado por `_ano_mes`

**Responsabilidades (limpeza + enriquecimento):**

| # | Transformação | Resultado |
|---|---------------|-----------|
| 1 | Trim de todas as strings | Remove espaços antes/depois |
| 2 | `{ñ class}` / `Nao Informado` / `""` → NULL | Nulidade explícita nos categóricos |
| 3 | Parse `banco` | `banco_codigo (Int)` + `banco_nome` |
| 4 | Parse `mun_pagto` | `mun_pagto_codigo` + `mun_pagto_sg_uf` + `mun_pagto_nome` |
| 5 | Parse `mun_resid` | `mun_resid_codigo` + `mun_resid_sg_uf` + `mun_resid_nome` |
| 6 | Derivar `sg_uf` | Sigla 2 letras via lookup de 27 estados |
| 7 | Cast `especie_codigo` | Double → Integer |
| 8 | Parse `meio_pagamento` | `meio_pag_codigo` + `meio_pag_descricao` |
| 9 | Derivar `ano_inicio` / `mes_inicio` | Inteiros a partir de `dt_inicio_validade` |
| 10 | Flag `fl_vl_zero` | True quando `vl_liquido <= 0` |
| 11 | Flag `fl_mesmo_municipio` | True quando município de pagto == residência |
| 12 | Metadado `_silver_ts` | Timestamp UTC do processamento silver |
| 13 | VACUUM | Remove arquivos físicos obsoletos das versões anteriores do Delta |

**Schema silver:** 29 colunas (14 bronze renomeadas + 12 derivadas + 3 metadados)

---

### 4.4 Gold (Ouro)

**Script:** `work/gold-beneficios.py`
**Fonte:** `s3a://prata/pda/beneficios-emitidos/`
**Destino:** `s3a://ouro/pda/beneficios-emitidos/` (4 tabelas)
**Formato:** Delta Lake, particionado por `_ano_mes`

**Tabelas geradas:**

| Tabela | Grain | Colunas-chave | Uso |
|--------|-------|---------------|-----|
| `fat_uf` | UF × mês | qtd, vl_total, vl_medio, rank, pct_do_brasil | Análise regional |
| `fat_especie` | Espécie × mês | qtd, vl_total, grupo_especie, rank | Análise por tipo de benefício |
| `fat_banco` | Banco × mês | qtd, vl_total, pct_do_total | Análise por instituição pagadora |
| `kpis_nacionais` | Mês | total, vl_total, vl_medio, vl_mediano, pct_feminino | Dashboard executivo |

**Princípios gold:**
- Granularidade declarada — cada tabela tem um grain explícito e documentado
- Sem dados brutos — apenas métricas e dimensões de análise
- Denormalizado — dimensões embutidas para leitura em 1 query (sem JOINs no BI)
- Validação de regras de negócio antes da escrita
- `PERSIST(MEMORY_AND_DISK)` — silver lida uma vez, reutilizada nas 4 tabelas

---

## 5. Decisões de Performance

| Decisão | Valor | Justificativa |
|---------|-------|---------------|
| `maxPartitionBytes` | 128 MB | Padrão oficial Spark — CSV 11 GB → ~90 partições de leitura |
| `shuffle.partitions` landing | `max(4, cores × 2)` | Simples, sem cache — 2 cores → 4 partitions |
| `shuffle.partitions` silver | `max(12, cores × 6)` | 41,5 M linhas em cache + groupBy → tasks de ~500 MB com 1 g executor |
| `coalesce(n_cores)` | 1 arquivo por core | Evita small files sem shuffle extra (valor único de `_ano_mes` no batch) |
| AQE | habilitado | `coalescePartitions` automático para tasks residuais |
| VACUUM | RETAIN 0 HOURS | Remove imediatamente arquivos de versões anteriores (ambiente dev) |

---

## 6. Estrutura no MinIO

```
landing/
  pda/beneficios-emitidos/202601/
    part-00000-*.parquet   (~185 MB)
    part-00001-*.parquet   (~185 MB)

bronze/
  pda/beneficios-emitidos/
    _delta_log/
    _ano_mes=202601/
      part-*.parquet

prata/  (silver)
  pda/beneficios-emitidos/
    _delta_log/
    _ano_mes=202601/
      part-*.parquet

ouro/  (gold)
  pda/beneficios-emitidos/
    fat_uf/         _delta_log/ + _ano_mes=202601/
    fat_especie/    _delta_log/ + _ano_mes=202601/
    fat_banco/      _delta_log/ + _ano_mes=202601/
    kpis_nacionais/ _delta_log/ + _ano_mes=202601/
```

---

## 7. Como Executar o Pipeline Completo

```bash
# 1. Landing: CSV → Parquet raw no MinIO
docker exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  /opt/bitnami/spark/work/landing-beneficios.py

# 2. Bronze: Landing Parquet → Delta Lake (tipos + snake_case)
docker exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  /opt/bitnami/spark/work/bronze-beneficios.py

# 3. Silver: Bronze → Delta Lake (limpo + enriquecido)
docker exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  /opt/bitnami/spark/work/silver-beneficios.py

# 4. Gold: Silver → 4 tabelas analíticas Delta
docker exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  /opt/bitnami/spark/work/gold-beneficios.py

# 5. Registrar tabelas no Trino (Hive Metastore)
docker exec trino-coordinator trino -f /etc/trino/init-trino.sql
```

---

## 8. Consultas de Referência (Trino)

```sql
-- KPIs nacionais da competência
SELECT * FROM delta.ouro.kpis_nacionais WHERE _ano_mes = '202601';

-- Top 5 UFs por valor total
SELECT sg_uf, uf, qtd_beneficios, vl_total_uf, rank_vl
FROM delta.ouro.fat_uf
WHERE _ano_mes = '202601'
ORDER BY rank_vl
LIMIT 5;

-- Distribuição por grupo de espécie
SELECT grupo_especie, SUM(qtd_beneficios) AS qtd, SUM(vl_total_especie) AS vl_total
FROM delta.ouro.fat_especie
WHERE _ano_mes = '202601'
GROUP BY grupo_especie
ORDER BY vl_total DESC;

-- Bancos pagadores
SELECT banco_nome, qtd_beneficios, vl_total_banco, pct_do_total
FROM delta.ouro.fat_banco
WHERE _ano_mes = '202601'
ORDER BY qtd_beneficios DESC;
```
