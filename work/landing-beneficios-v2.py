"""
Pipeline Landing Zone - Benefícios Emitidos PDA 2025-2027  [v2 — otimizado]
Fonte : Portal de Dados Abertos do Governo Federal
Destino: MinIO  s3a://landing/pda/beneficios-emitidos/202601/

Fluxo:
  1. Download ZIP da fonte publica (pula se CSV ja existe localmente)
  2. Extrai CSV para cache local em /opt/bitnami/spark/work/dados-abertos/
  3. Le CSV com Spark (schema raw, sem inferencia de tipo)
  4. Cria bucket 'landing' no MinIO (via Hadoop S3A) se nao existir
  5. Grava como Parquet em s3a://landing/pda/beneficios-emitidos/202601/
  6. Valida lendo de volta do MinIO em passe unico (groupBy persistido)

Otimizacoes aplicadas em v2 vs v1:
  - S3A multipart upload (128 MB), conexoes maximas (100), fast upload com bytebuffer
  - maxPartitionBytes elevado para 256m (melhor throughput em arquivo 11 GB com poucos cores)
  - Validacao em passe unico: row_count derivado da soma do groupBy persistido,
    eliminando o segundo scan full do count() independente
"""
import io
import os
import time
import urllib.request
import zipfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuracoes
# ---------------------------------------------------------------------------
URL = (
    "https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/"
    "PDA_2025_2027/Grupos_de_dados/Benef%C3%ADcios+emitidos/"
    "D.SDA.PDA.003.EMI.202601.CSV.ZIP"
)

CACHE_DIR   = "/opt/bitnami/spark/work/dados-abertos"
CSV_NAME    = "D.SDA.PDA.003.EMI.202601.csv"
csv_path    = os.path.join(CACHE_DIR, CSV_NAME)

BUCKET      = "landing"
MINIO_PATH  = f"s3a://{BUCKET}/pda/beneficios-emitidos/202601/"

os.makedirs(CACHE_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# 1. Download + extração (pula se CSV já existe no cache local)
# ---------------------------------------------------------------------------
print("=" * 60)
if os.path.exists(csv_path):
    size_mb = os.path.getsize(csv_path) / (1024 ** 2)
    print(f"1. Cache encontrado: {csv_path} ({size_mb:.1f} MB) — pulando download")
else:
    print("1. Baixando ZIP da fonte pública...")
    t0 = time.time()
    zip_bytes = urllib.request.urlopen(URL).read()
    print(f"   Baixado: {len(zip_bytes) / (1024**2):.1f} MB em {time.time()-t0:.1f}s")

    print("2. Extraindo CSV...")
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_files = [f for f in zf.namelist() if f.lower().endswith(".csv")]
        zf.extract(csv_files[0], CACHE_DIR)
        csv_path = os.path.join(CACHE_DIR, csv_files[0])
    print(f"   Extraído: {csv_path} ({os.path.getsize(csv_path)/(1024**2):.1f} MB)")
    del zip_bytes

# ---------------------------------------------------------------------------
# 2. Detectar separador
# ---------------------------------------------------------------------------
with open(csv_path, "r", encoding="latin-1") as f:
    header = f.readline()
sep = next((s for s in [";", ",", "\t", "|"] if header.count(s) > 2), ";")
print(f"\n3. Separador detectado: '{sep}'")

# ---------------------------------------------------------------------------
# 3. SparkSession — usa spark-defaults.conf para conexão S3A/MinIO base;
#    configs adicionais de performance S3A são aplicadas aqui no builder:
#    - multipart.size 128 MB: cada parte do upload multipart tem 128 MB,
#      reduzindo o numero de requests HTTP para arquivos grandes
#    - connection.maximum 100: pool de conexoes maior evita contenção ao
#      escrever varios arquivos Parquet em paralelo
#    - fast.upload true + bytebuffer: buffer em memória heap antes de enviar,
#      eliminando gravações temporárias em disco local durante o upload S3A
# ---------------------------------------------------------------------------
print("\n4. Iniciando SparkSession...")
spark = (
    SparkSession.builder
    .appName("Landing-PDA-Beneficios-202601-v2")
    # v2: maxPartitionBytes elevado para 256m — com arquivo de 11 GB e poucos
    # cores, partições maiores reduzem overhead de scheduling e aumentam
    # throughput de leitura sequencial do CSV local
    .config("spark.sql.files.maxPartitionBytes", "256m")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # v2: configs S3A para upload multipart de alta performance no MinIO
    .config("spark.hadoop.fs.s3a.multipart.size", "134217728")       # 128 MB por parte
    .config("spark.hadoop.fs.s3a.connection.maximum", "100")         # pool de 100 conexoes
    .config("spark.hadoop.fs.s3a.fast.upload", "true")               # upload assíncrono
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")  # buffer em heap
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

total_cores     = spark.sparkContext.defaultParallelism
shuffle_parts   = max(4, total_cores * 2)
n_output_files  = max(1, total_cores)

spark.conf.set("spark.sql.shuffle.partitions", shuffle_parts)

print(f"   Cores alocados      : {total_cores}")
print(f"   shuffle.partitions  : {shuffle_parts}  (cores × 2)")
print(f"   Arquivos de saída   : {n_output_files} (coalesce = 1 por core)")

# ---------------------------------------------------------------------------
# 4. Criar bucket 'landing' no MinIO (via Hadoop FileSystem S3A)
# ---------------------------------------------------------------------------
print(f"\n5. Verificando bucket '{BUCKET}' no MinIO...")
jvm         = spark.sparkContext._jvm
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
uri         = jvm.java.net.URI.create(f"s3a://{BUCKET}")
fs          = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
bucket_path = jvm.org.apache.hadoop.fs.Path(f"s3a://{BUCKET}/")

if not fs.exists(bucket_path):
    fs.mkdirs(bucket_path)
    print(f"   Bucket '{BUCKET}' criado.")
else:
    print(f"   Bucket '{BUCKET}' já existe.")

# ---------------------------------------------------------------------------
# 5. Ler CSV — landing = raw, inferSchema=False (preserva dado original)
# ---------------------------------------------------------------------------
print("\n6. Lendo CSV com Spark...")
df = spark.read.csv(
    csv_path,
    header=True,
    inferSchema=False,
    sep=sep,
    encoding="ISO-8859-1",
)

# Desambiguar colunas duplicadas ('Espécie' aparece 2x no dataset)
seen, new_cols = {}, []
for c in df.columns:
    if c in seen:
        seen[c] += 1
        new_cols.append(f"{c}_{seen[c]}")
    else:
        seen[c] = 0
        new_cols.append(c)
df = df.toDF(*new_cols)

print(f"   Colunas ({len(df.columns)}): {df.columns}")

# ---------------------------------------------------------------------------
# 6. Gravar no MinIO como Parquet (overwrite para idempotência)
# ---------------------------------------------------------------------------
print(f"\n7. Gravando em {MINIO_PATH} ...")
t0 = time.time()
df.coalesce(n_output_files).write.mode("overwrite").parquet(MINIO_PATH)
print(f"   Gravação concluída em {time.time()-t0:.1f}s")

# ---------------------------------------------------------------------------
# 7. Validação em passe único — v2 elimina o segundo scan full do count()
#    independente do v1. Estratégia:
#    1. Lê os dados de volta do MinIO
#    2. Executa groupBy("UF") com persist() — materializa o resultado em memória
#    3. Deriva row_count somando a coluna "count" do agregado já calculado
#    4. Exibe top 10 UFs
#    5. Libera o cache com unpersist()
#    Resultado: apenas 1 scan sobre os dados Parquet em vez de 2
# ---------------------------------------------------------------------------
print(f"\n8. Validando leitura do MinIO...")
df_val = spark.read.parquet(MINIO_PATH)

top_uf = (
    df_val.groupBy("UF")
    .agg(F.count("*").alias("count"))
    .orderBy(F.col("count").desc())
    .persist()
)

# Trigger da materialização + derivação do total sem scan adicional
row_count = top_uf.agg(F.sum("count")).collect()[0][0]

print(f"\n{'='*60}")
print("   RESULTADOS DA VALIDAÇÃO")
print(f"{'='*60}")
print(f"\n   Linhas no MinIO : {row_count:,}")
print(f"   Destino         : {MINIO_PATH}")
print(f"\n   Schema:")
df_val.printSchema()

print("\n   Top 10 UFs por volume de benefícios:")
top_uf.show(10, truncate=False)

top_uf.unpersist()

print(f"\n{'='*60}")
print("   LANDING ZONE CONCLUÍDA COM SUCESSO!")
print(f"{'='*60}\n")

spark.stop()
