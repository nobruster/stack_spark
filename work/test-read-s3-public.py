"""
Leitura de CSV publico do portal de dados abertos + gravacao no MinIO landing zone.
Fonte: Beneficios emitidos - PDA 2025-2027

Como executar:
  docker exec spark-master \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/bitnami/spark/work/test-read-s3-public.py
"""
import io
import os
import time
import urllib.request
import zipfile

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ---------------------------------------------------------------------------
# Configuracoes
# ---------------------------------------------------------------------------
URL = (
    "https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/"
    "PDA_2025_2027/Grupos_de_dados/Benef%C3%ADcios+emitidos/"
    "D.SDA.PDA.003.EMI.202601.CSV.ZIP"
)

EXTRACT_DIR = "/opt/bitnami/spark/work/dados-abertos"
CSV_NAME    = "D.SDA.PDA.003.EMI.202601.csv"
csv_path    = os.path.join(EXTRACT_DIR, CSV_NAME)

BUCKET     = "landing"
MINIO_PATH = f"s3a://{BUCKET}/pda/beneficios-emitidos/202601/"

os.makedirs(EXTRACT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# 1. Download do ZIP (pula se CSV ja existe no cache local)
# ---------------------------------------------------------------------------
print("=" * 60)
if os.path.exists(csv_path):
    csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    print(f"1. CSV ja existe: {csv_path} ({csv_size_mb:.1f} MB) - pulando download")
else:
    print("1. Baixando arquivo ZIP...")
    start = time.time()
    zip_bytes = urllib.request.urlopen(URL).read()
    size_mb = len(zip_bytes) / (1024 * 1024)
    elapsed = time.time() - start
    print(f"   Download concluido: {size_mb:.1f} MB em {elapsed:.1f}s")

    print("\n2. Extraindo CSV do ZIP...")
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        file_list = zf.namelist()
        print(f"   Arquivos no ZIP: {file_list}")
        csv_files = [f for f in file_list if f.lower().endswith(".csv")]
        if not csv_files:
            print("   ERRO: Nenhum CSV encontrado no ZIP!")
            exit(1)
        csv_name = csv_files[0]
        zf.extract(csv_name, EXTRACT_DIR)
        csv_path = os.path.join(EXTRACT_DIR, csv_name)
        csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
        print(f"   CSV extraido: {csv_name} ({csv_size_mb:.1f} MB)")
    del zip_bytes

# ---------------------------------------------------------------------------
# 2. Detectar separador
# ---------------------------------------------------------------------------
print("\n3. Detectando separador do CSV...")
with open(csv_path, "r", encoding="latin-1") as f:
    header_line = f.readline()
    for sep in [";", ",", "\t", "|"]:
        cnt = header_line.count(sep)
        if cnt > 2:
            print(f"   Separador detectado: '{sep}' ({cnt} ocorrencias no header)")
            break
    else:
        sep = ";"
        print(f"   Usando separador padrao: '{sep}'")

print(f"   Colunas: {header_line.strip().split(sep)}")

# ---------------------------------------------------------------------------
# 3. SparkSession (usa spark-defaults.conf — S3A/MinIO ja configurado)
# ---------------------------------------------------------------------------
print("\n4. Criando SparkSession e lendo CSV...")
spark = (
    SparkSession.builder
    .appName("Teste-DadosAbertos-Beneficios")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv(
    csv_path,
    header=True,
    inferSchema=True,
    sep=sep,
    encoding="ISO-8859-1",
)

# Desambiguar colunas duplicadas ('Especie' aparece 2x no dataset)
seen, new_cols = {}, []
for c in df.columns:
    if c in seen:
        seen[c] += 1
        new_cols.append(f"{c}_{seen[c]}")
    else:
        seen[c] = 0
        new_cols.append(c)
df = df.toDF(*new_cols)

# ---------------------------------------------------------------------------
# 4. Analise local
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("5. RESULTADOS")
print("=" * 60)

print(f"\n   Total de colunas: {len(df.columns)}")
print("\n   Schema:")
df.printSchema()

print("\n   Primeiras 10 linhas:")
df.show(10, truncate=30)

print("\n   Contando registros...")
row_count = df.count()
print(f"   Total de linhas: {row_count:,}")

print("\n   Amostra de valores unicos por UF:")
df.groupBy("UF").count().orderBy(col("count").desc()).show(10)

# ---------------------------------------------------------------------------
# 5. Criar bucket 'landing' no MinIO (via Hadoop S3A) se nao existir
# ---------------------------------------------------------------------------
print(f"\n6. Verificando bucket '{BUCKET}' no MinIO...")
jvm         = spark.sparkContext._jvm
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
uri         = jvm.java.net.URI.create(f"s3a://{BUCKET}")
fs          = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
bucket_path = jvm.org.apache.hadoop.fs.Path(f"s3a://{BUCKET}/")

if not fs.exists(bucket_path):
    fs.mkdirs(bucket_path)
    print(f"   Bucket '{BUCKET}' criado.")
else:
    print(f"   Bucket '{BUCKET}' ja existe.")

# ---------------------------------------------------------------------------
# 6. Gravar no MinIO como Parquet (landing zone — dados raw)
# ---------------------------------------------------------------------------
print(f"\n7. Gravando em {MINIO_PATH} ...")
t0 = time.time()
df.write.mode("overwrite").parquet(MINIO_PATH)
print(f"   Gravacao concluida em {time.time() - t0:.1f}s")

# ---------------------------------------------------------------------------
# 7. Validar lendo de volta do MinIO
# ---------------------------------------------------------------------------
print(f"\n8. Validando leitura do MinIO...")
df_val    = spark.read.parquet(MINIO_PATH)
val_count = df_val.count()
print(f"   Linhas confirmadas no MinIO: {val_count:,}")
print(f"   Destino: {MINIO_PATH}")

print("\n" + "=" * 60)
print("   CONCLUIDO COM SUCESSO!")
print("=" * 60)

spark.stop()
