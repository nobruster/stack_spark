"""
Teste de leitura de arquivo CSV.ZIP publico do portal de dados abertos do governo.
Fonte: Beneficios emitidos - PDA 2025-2027


docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/work/test-read-s3-public.py

docker exec -it -e HOME=/root spark-master \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/bitnami/spark/work/test-read-s3-public.py

"""
import urllib.request
import zipfile
import io
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

URL = (
    "https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/"
    "PDA_2025_2027/Grupos_de_dados/Benef%C3%ADcios+emitidos/"
    "D.SDA.PDA.003.EMI.202601.CSV.ZIP"
)

EXTRACT_DIR = "/opt/bitnami/spark/work/dados-abertos"
os.makedirs(EXTRACT_DIR, exist_ok=True)

csv_path = os.path.join(EXTRACT_DIR, "D.SDA.PDA.003.EMI.202601.csv")

# --- 1. Download do ZIP (pula se ja existe) ---
print("=" * 60)
if os.path.exists(csv_path):
    csv_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    print(f"1. CSV ja existe: {csv_path} ({csv_size_mb:.1f} MB) - pulando download")
else:
    print("1. Baixando arquivo ZIP...")
    start = time.time()
    response = urllib.request.urlopen(URL)
    zip_bytes = response.read()
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

# --- 3. Detectar separador ---
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

# --- 4. Ler com PySpark (local mode - arquivo no driver) ---
print("\n4. Criando SparkSession (local mode) e lendo CSV...")
spark = SparkSession.builder \
    .appName("Teste-DadosAbertos-Beneficios") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

df = spark.read.csv(
    csv_path,
    header=True,
    inferSchema=True,
    sep=sep,
    encoding="ISO-8859-1"
)

# --- 5. Resultados ---
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

print("\n   Teste concluido com SUCESSO!")

spark.stop()
