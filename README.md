# Stack de Dados - Spark + MinIO + Dremio

Stack completa para processamento e analise de dados com Apache Spark, MinIO (object storage) e Dremio (query engine), tudo rodando em containers Docker.

---

## Arquitetura

```
          +-------------------+    +-------------------+
          | Jupyter-1 (8888)  |    | Jupyter-2 (8889)  |
          |  (Spark Driver)   |    |  (Spark Driver)   |
          +--------+----------+    +--------+----------+
                   |                        |
                   +--------+---------------+
                            |
                   +--------v----------+
                    | Spark Master (8090)|
                    +--------+----------+
                             |
              +--------------+--------------+
              |              |              |
     +--------v---+  +------v-----+  +-----v------+
     | Worker 1   |  | Worker 2   |  | Worker 3   |
     | (8081)     |  | (8082)     |  | (8083)     |
     +------------+  +------------+  +------------+

     +------------+  +------------+  +------------+  +------------+
     |  MinIO 1   |  |  MinIO 2   |  |  MinIO 3   |  |  MinIO 4   |
     | (9000/9001)|  |            |  |            |  |            |
     +------------+  +------------+  +------------+  +------------+

     +-------------------+       +-------------------+
     | Spark History     |       |  Dremio (9047)    |
     | (18080)           |       |                   |
     +-------------------+       +-------------------+
```

**Fluxo de dados:**
1. Arquivos JSON sao carregados no MinIO (bucket `landing`)
2. Jupyter executa notebooks PySpark que leem do MinIO via protocolo S3A
3. Spark Master distribui as tarefas entre os 3 Workers
4. Dados processados sao gravados no MinIO (bucket `processing`) em formato Parquet e Delta Lake
5. Dremio conecta no MinIO e permite consultas SQL sobre os dados processados

---

## Pre-requisitos

### Software necessario

| Software | Versao minima | Como verificar |
|---|---|---|
| Docker | 20.10+ | `docker --version` |
| Docker Compose | 2.0+ (plugin) | `docker compose version` |

> **Nota:** O Docker Compose V2 vem integrado como plugin do Docker (`docker compose`). Se voce usa a versao antiga (`docker-compose` com hifen), os comandos sao os mesmos, basta trocar `docker compose` por `docker-compose`.

### Recursos de hardware

| Recurso | Minimo | Recomendado |
|---|---|---|
| RAM | 16 GB | 24 GB |
| CPU | 4 cores | 8 cores |
| Disco | 10 GB livres | 20 GB livres |

A RAM e distribuida assim:
- Spark Workers: 3 x 2.5 GB = 7.5 GB
- Dremio: ate 8 GB (4 GB heap + 2 GB direct + overhead)
- MinIO: ~500 MB por node = 2 GB
- Spark Master: 1 GB
- Spark History: 512 MB
- Jupyter 1 + Jupyter 2: 2 x 2 GB = 4 GB

### Instalacao do Docker (se ainda nao tem)

**Ubuntu/Debian:**
```bash
# Atualizar pacotes
sudo apt-get update

# Instalar Docker
sudo apt-get install -y docker.io docker-compose-plugin

# Adicionar seu usuario ao grupo docker (evita precisar de sudo)
sudo usermod -aG docker $USER

# Reiniciar a sessao (logout/login) para aplicar o grupo
# Depois verificar:
docker --version
docker compose version
```

**Windows (WSL2):**
1. Instale o Docker Desktop: https://www.docker.com/products/docker-desktop/
2. Nas configuracoes do Docker Desktop, habilite a integracao com WSL2
3. Abra o terminal WSL e verifique: `docker --version`

---

## Estrutura do Projeto

```
stack-prev/
├── docker-compose.yml          # Orquestrador de todos os 13 containers
├── Dockerfile.spark            # Imagem customizada do Spark (base bitnami)
├── requirements.txt            # Dependencias Python instaladas na imagem
├── README.md                   # Este arquivo
├── config/
│   └── spark/
│       ├── spark-defaults.conf     # Configs globais do Spark (master, S3A, limites)
│       ├── log4j2.properties       # Nivel de log (warn para reduzir ruido)
│       ├── jupyter-entrypoint.sh   # Entrypoint dos containers Jupyter
│       └── jars/                   # JARs extras copiados para o Spark
│           ├── delta-spark_2.12-3.2.0.jar
│           ├── delta-storage-3.2.0.jar
│           ├── iceberg-spark-runtime-3.5_2.12-1.9.1.jar
│           ├── postgresql-9.4.1207.jar
│           └── spark-measure_2.12-0.24.jar
├── data/                       # Dados persistentes (bind mounts)
│   ├── minio1/                 # Dados MinIO node 1
│   ├── minio2/                 # Dados MinIO node 2
│   ├── minio3/                 # Dados MinIO node 3
│   ├── minio4/                 # Dados MinIO node 4
│   ├── dremio/                 # Config e metadados do Dremio
│   ├── dremio-spill/           # Spill to disk do Dremio
│   └── spark-events/           # Historico de jobs Spark
└── work/                       # Volume compartilhado: notebooks e scripts
    ├── env                     # Credenciais do MinIO para o SDK Python
    ├── 01-Init.ipynb           # Notebook de testes basicos do Spark
    ├── 01-buckets.ipynb        # Notebook de gerenciamento de buckets MinIO
    ├── etl.ipynb               # Notebook ETL: JSON → Parquet + Delta
    └── medallion-pipeline.ipynb # Pipeline Medallion: Bronze → Prata → Ouro
```

### Descricao de cada arquivo

**docker-compose.yml** — Define 13 containers:
- `spark-master` — coordena o cluster Spark
- `spark-worker-1`, `spark-worker-2`, `spark-worker-3` — executam as tarefas
- `spark-history` — historico de jobs Spark
- `jupyter-1`, `jupyter-2` — IDEs para notebooks (drivers Spark independentes)
- `minio1`, `minio2`, `minio3`, `minio4` — cluster de object storage
- `dremio` — motor SQL para consultas

**Dockerfile.spark** — Imagem customizada baseada em `bitnami/spark:3.5.5`. Instala Python, pip, JARs extras, libs Python (PySpark, Pandas, Delta Lake, etc).

**spark-defaults.conf** — Configuracoes pre-definidas para que qualquer SparkSession criada no Jupyter ja se conecte automaticamente ao cluster e ao MinIO. Inclui limites de recursos por aplicacao (`spark.cores.max`, `spark.executor.memory`, `spark.driver.memory`) para proteger o cluster.

**jupyter-entrypoint.sh** — Script de inicializacao dos containers Jupyter. Configura automaticamente o `spark.driver.host` com o nome do container, permitindo multiplos Jupiters simultaneos.

**log4j2.properties** — Nivel de log em `warn` para evitar excesso de mensagens no Jupyter.

---

## Passo 1 — Clonar/Baixar o Projeto

Se voce recebeu o projeto como ZIP, extraia-o. Se esta usando Git:

```bash
git clone <url-do-repositorio>
cd stack-prev
```

Verifique que todos os arquivos existem:

```bash
ls -la
# Deve mostrar: docker-compose.yml, Dockerfile.spark, requirements.txt, config/, work/

ls config/spark/jars/
# Deve mostrar os 5 arquivos .jar
```

> **Importante:** Os arquivos `.jar` na pasta `config/spark/jars/` sao essenciais. Sem eles, o Spark nao consegue ler/escrever Delta Lake e Iceberg. Certifique-se de que eles estao presentes antes de continuar.

### Criar as pastas de dados persistentes

```bash
mkdir -p data/minio1 data/minio2 data/minio3 data/minio4 data/dremio data/dremio-spill data/spark-events
```

Dar permissao para o Dremio (roda com usuario interno, nao root):

```bash
chmod -R 777 data/dremio data/dremio-spill
```

> **Por que isso e necessario?** O Dremio roda com um usuario especifico dentro do container (nao root). Se a pasta `data/dremio` nao tiver permissao de escrita, o Dremio falha com o erro `path /opt/dremio/data is not writable`.

---

## Passo 2 — Subir a Stack

```bash
docker compose up -d --build
```

O que este comando faz:
1. **Constroi** a imagem `Dockerfile.spark` (instala Python, JARs, libs)
2. **Inicia** todos os 13 containers em background (`-d`)
3. O primeiro build demora **5-10 minutos** (download das imagens + pip install)
4. Builds seguintes sao rapidos (cache do Docker)

### Acompanhar o progresso

```bash
# Ver status de todos os containers
docker compose ps
```

Todos os containers devem estar com status **Up**. Exemplo de saida esperada:

```
NAME             IMAGE                  STATUS          PORTS
spark-master     stack-prev-spark-...   Up              0.0.0.0:7077->7077/tcp, 0.0.0.0:8090->8080/tcp
spark-worker-1   stack-prev-spark-...   Up              0.0.0.0:8081->8081/tcp
spark-worker-2   stack-prev-spark-...   Up              0.0.0.0:8082->8081/tcp
spark-worker-3   stack-prev-spark-...   Up              0.0.0.0:8083->8081/tcp
spark-history    stack-prev-spark-...   Up              0.0.0.0:18080->18080/tcp
jupyter-1        stack-prev-spark-...   Up              0.0.0.0:8888->8888/tcp
jupyter-2        stack-prev-spark-...   Up              0.0.0.0:8889->8888/tcp
minio1           minio/minio            Up (healthy)    0.0.0.0:9000->9000/tcp, 0.0.0.0:9001->9001/tcp
minio2           minio/minio            Up (healthy)
minio3           minio/minio            Up (healthy)
minio4           minio/minio            Up (healthy)
dremio           dremio/dremio-oss      Up              0.0.0.0:9047->9047/tcp, ...
```

Se algum container nao esta **Up**, veja os logs:

```bash
docker compose logs <nome-do-servico> --tail 50
# Exemplo:
docker compose logs jupyter-1 --tail 50
docker compose logs spark-master --tail 50
```

---

## Passo 3 — Verificar os Servicos

Abra cada URL no navegador para confirmar que esta tudo rodando:

| Servico | URL | O que voce deve ver |
|---|---|---|
| Spark Master | http://localhost:8090 | UI do Spark mostrando 3 Workers registrados |
| Spark Worker 1 | http://localhost:8081 | Detalhes do worker 1 (2 cores, 2 GB RAM) |
| Spark Worker 2 | http://localhost:8082 | Detalhes do worker 2 |
| Spark Worker 3 | http://localhost:8083 | Detalhes do worker 3 |
| Spark History | http://localhost:18080 | Lista de aplicacoes Spark executadas |
| Jupyter 1 | http://localhost:8888/?token=spark123 | Interface do Jupyter 1 (requer token) |
| Jupyter 2 | http://localhost:8889/?token=spark123 | Interface do Jupyter 2 (requer token) |
| MinIO Console | http://localhost:9001 | Tela de login do MinIO |
| MinIO API (S3) | http://localhost:9000 | Resposta XML (endpoint da API S3) |
| Dremio | http://localhost:9047 | Tela de setup/login do Dremio |

> **Dica:** No Spark Master UI (porta 8090), confirme que os 3 workers aparecem na secao "Workers". Se aparecer "0 workers", aguarde 30 segundos e atualize a pagina — os workers levam alguns segundos para se registrar.

---

## Passo 4 — Configurar MinIO (Object Storage)

O MinIO e o armazenamento de objetos da stack. Funciona como um Amazon S3 local. Voce precisa criar buckets (pastas raiz) para organizar os dados.

### 4.1 — Fazer login no MinIO Console

1. Abra http://localhost:9001
2. Preencha as credenciais:
   - **Username:** `minioadmin`
   - **Password:** `minioadmin`
3. Clique **Login**

### 4.2 — Criar o bucket `landing`

O bucket `landing` armazena os dados brutos (arquivos JSON que serao processados):

1. No menu lateral esquerdo, clique em **Buckets**
2. Clique no botao **Create Bucket** (canto superior direito)
3. Em **Bucket Name**, digite: `landing`
4. Clique **Create Bucket**

### 4.3 — Criar o bucket `processing`

O bucket `processing` armazena os dados ja transformados (Parquet e Delta):

1. Repita o processo acima
2. Em **Bucket Name**, digite: `processing`
3. Clique **Create Bucket**

### 4.4 — Fazer upload dos dados JSON

Para o ETL funcionar, voce precisa ter arquivos JSON no bucket `landing`:

1. No menu lateral, clique em **Object Browser**
2. Clique no bucket **landing**
3. Clique em **Upload** > **Upload File**
4. Selecione seu(s) arquivo(s) `.json`
5. Clique **Upload**

> **Formato esperado:** Os arquivos JSON devem ser do tipo JSON Lines (um JSON por linha) ou um array JSON. O Spark infere o schema automaticamente.

---

## Passo 5 — Usar o Jupyter com Spark

### 5.1 — Acessar o Jupyter

A stack tem **2 instancias Jupyter** rodando simultaneamente, cada uma independente:

| Instancia | URL | Container |
|---|---|---|
| Jupyter 1 | http://localhost:8888/?token=spark123 | `jupyter-1` |
| Jupyter 2 | http://localhost:8889/?token=spark123 | `jupyter-2` |

Ambas compartilham o mesmo diretorio `work/` (notebooks visiveis nos dois). O token padrao e `spark123`.

**Para alterar o token**, defina a variavel `JUPYTER_TOKEN` antes de subir a stack:

```bash
# Opcao 1: inline no terminal
JUPYTER_TOKEN=meuTokenSecreto docker compose up -d

# Opcao 2: criar um arquivo .env na raiz do projeto
echo "JUPYTER_TOKEN=meuTokenSecreto" > .env
docker compose up -d
```

> **Multi-Jupyter:** Cada Jupyter cria uma SparkSession independente no cluster. O `jupyter-entrypoint.sh` configura automaticamente o `spark.driver.host` com o nome do container (jupyter-1 ou jupyter-2), permitindo que ambos se comuniquem com os workers sem conflito.

### 5.2 — Multiplos notebooks no mesmo Jupyter

Voce pode abrir **quantos notebooks quiser** dentro do mesmo Jupyter (ex: porta 8888). Todos rodam no mesmo container.

| Cenario | O que acontece |
|---|---|
| 2 notebooks, **mesmo** `appName` | Compartilham a mesma SparkSession (mesmos 2 cores) |
| 2 notebooks, `appName` **diferente** | 2 SparkSessions independentes, competem pelos mesmos 2 cores |
| 1 notebook no Jupyter-1 + 1 no Jupyter-2 | 2 drivers independentes, cada um com seus 2 cores (total 4 de 6) |

Para ter sessoes Spark independentes no mesmo Jupyter, use `appName` diferente em cada notebook:

```python
# Notebook A
spark = SparkSession.builder.appName("etl-bronze").getOrCreate()

# Notebook B (mesmo Jupyter, sessao separada)
spark = SparkSession.builder.appName("etl-gold").getOrCreate()
```

> **Dica:** Se ambos usarem o mesmo `appName`, o `getOrCreate()` retorna a mesma sessao — o que na maioria dos casos e o comportamento desejado (compartilham recursos). Use o **Jupyter-2** (porta 8889) quando precisar de isolamento real: drivers separados, recursos separados, sem interferencia.

### 5.3 — Criar uma SparkSession (modo simples)

Crie um novo notebook: **New** > **Python 3**

O Spark ja vem pre-configurado. Basta criar a sessao:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("meu-app") \
    .getOrCreate()
```

> **Por que nao preciso configurar `.master()` ou credenciais S3?**
> Tudo ja esta definido no arquivo `spark-defaults.conf`, que e carregado automaticamente. Isso inclui:
> - `spark.master` → conecta ao cluster (`spark://spark-master:7077`)
> - `spark.driver.host` → configurado pelo `jupyter-entrypoint.sh` com o nome do container
> - `spark.hadoop.fs.s3a.*` → credenciais e endpoint do MinIO
> - `spark.cores.max` → limita cada app a 2 cores (protege o cluster)
> - `spark.executor.memory` → limita cada executor a 1 GB

### 5.3 — Verificar a conexao com o cluster

Apos criar a SparkSession, verifique no Spark Master UI (http://localhost:8090):

1. Na secao **Running Applications**, deve aparecer sua aplicacao
2. O nome sera o que voce definiu em `.appName()`
3. Os 3 workers devem estar alocados

Se a aplicacao nao aparece, execute no notebook:

```python
print(spark.sparkContext.master)
# Deve imprimir: spark://spark-master:7077
```

### 5.4 — Ler dados JSON do MinIO

```python
# Ler todos os arquivos JSON do bucket landing
df = spark.read \
    .format("json") \
    .option("inferSchema", "true") \
    .json("s3a://landing/*.json")

# Ver as primeiras linhas
df.show()

# Contar registros
print(f"Total de registros: {df.count()}")

# Ver o schema inferido
df.printSchema()
```

O prefixo `s3a://` e o protocolo que o Spark usa para acessar o MinIO.

### 5.5 — Gravar dados em Parquet

```python
df.write \
    .mode("overwrite") \
    .parquet("s3a://processing/device/parquet")
```

### 5.6 — Gravar dados em Delta Lake

Para usar Delta Lake, adicione as configuracoes na SparkSession:

```python
spark = SparkSession.builder \
    .appName("etl-delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .getOrCreate()

df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("s3a://processing/device/delta")
```

> **Importante:** A config `S3SingleDriverLogStore` e necessaria para gravar Delta Lake no MinIO (S3). Sem ela, a escrita falha.

### 5.7 — Verificar os dados no MinIO

Apos gravar, voce pode verificar no MinIO Console:

1. Abra http://localhost:9001
2. Va em **Object Browser** > **processing**
3. Voce vera as pastas:
   - `device/parquet/` — arquivos `.parquet` + `_SUCCESS`
   - `device/delta/` — arquivos `.parquet` + pasta `_delta_log/`

---

## Passo 6 — Configurar Dremio (Query Engine SQL)

O Dremio permite consultar os dados do MinIO usando SQL puro, sem precisar de codigo Python.

### 6.1 — Primeiro acesso ao Dremio

1. Abra http://localhost:9047
2. Na primeira vez, o Dremio pede para criar uma conta de administrador
3. Preencha:
   - **First Name:** seu nome
   - **Last Name:** seu sobrenome
   - **Email:** seu email
   - **Username:** `admin` (ou o que preferir)
   - **Password:** defina uma senha (minimo 8 caracteres)
4. Clique **Next** / **Save**

### 6.2 — Conectar Dremio ao MinIO

1. Na tela inicial do Dremio, clique em **Add Source** (botao com `+` no painel esquerdo)
2. Selecione **Amazon S3** na lista de conectores

3. **Aba General:**

   | Campo | Valor |
   |---|---|
   | Name | `dremio_minio` |
   | Authentication | **AWS Access Key** (selecionado) |
   | AWS Access Key | `minioadmin` |
   | AWS Access Secret | `minioadmin` |
   | IAM Role to Assume | (deixar vazio) |
   | Encrypt connection | **Desmarcar** (checkbox) |

   Na secao **Public Buckets**, clique em **Add bucket** e adicione os buckets que voce criou no MinIO. Exemplo:

   | Buckets |
   |---|
   | `landing` |
   | `bronze` |
   | `prata` |
   | `ouro` |

   > Os nomes dos buckets devem corresponder exatamente aos criados no MinIO Console (Passo 4).

4. **Aba Advanced Options:**

   Marque os seguintes checkboxes:

   | Opcao | Valor |
   |---|---|
   | Enable asynchronous access when possible | **Marcado** |
   | Enable compatibility mode | **Marcado** |
   | Apply requester-pays to S3 requests | Desmarcado |
   | Enable file status check | **Marcado** |
   | Enable partition column inference | Desmarcado |

   Demais campos:

   | Campo | Valor |
   |---|---|
   | Root Path | `/` |
   | Default CTAS Format | **PARQUET** |

   Na secao **Connection Properties**, clique em **Add property** tres vezes e adicione:

   | Property Name | Property Value |
   |---|---|
   | `fs.s3a.endpoint` | `minio1:9000` |
   | `fs.s3a.path.style.access` | `true` |
   | `dremio.s3.compact` | `true` |

   Na secao **Allowlisted buckets**, clique em **Add bucket** e adicione os buckets que o Dremio pode acessar:

   | Allowlisted buckets |
   |---|
   | `landing` |

   > Adicione aqui todos os buckets que o Dremio precisa ler. Se nao adicionar nenhum, o Dremio tera acesso a todos.

   Na secao **Cache Options**:

   | Campo | Valor |
   |---|---|
   | Enable local caching when possible | **Marcado** |
   | Max percent of total available cache space | `50` |

5. Clique **Save**

Se a conexao for bem-sucedida, voce vera a source `dremio_minio` no painel esquerdo. Clique nela para ver os buckets (`landing`, `bronze`, `prata`, `ouro`).

### 6.3 — Promover pasta para tabela (Parquet)

Para consultar dados Parquet via SQL, voce precisa "promover" a pasta para tabela:

1. No painel esquerdo, navegue: **dremio_minio** > **processing** > **device** > **parquet**
2. Ao lado da pasta `parquet`, clique no icone de **promover** (icone de tabela/dataset)
3. Em **Format**, selecione **Parquet**
4. Clique **Save**

Agora consulte via SQL:

```sql
SELECT * FROM dremio_minio.processing.device.parquet;
```

### 6.4 — Promover pasta para tabela (Delta Lake)

1. No painel esquerdo, navegue: **dremio_minio** > **processing** > **device** > **delta**
2. Clique no icone de **promover**
3. Em **Format**, selecione **Delta Lake**
4. Clique **Save**

Agora consulte via SQL:

```sql
SELECT * FROM dremio_minio.processing.device.delta;
```

### 6.5 — Exemplos de consultas SQL no Dremio

```sql
-- Contar registros
SELECT COUNT(*) FROM dremio_minio.processing.device.delta;

-- Agrupar por fabricante
SELECT manufacturer, COUNT(*) as total
FROM dremio_minio.processing.device.delta
GROUP BY manufacturer
ORDER BY total DESC;

-- Filtrar por plataforma
SELECT * FROM dremio_minio.processing.device.delta
WHERE platform = 'Android'
LIMIT 100;
```

### 6.6 — Dremio com arquivos grandes

O Dremio esta configurado para lidar com arquivos de varios gigabytes:

- **Heap Memory:** 4 GB — para processamento de queries
- **Direct Memory:** 2 GB — para leitura columnar (Parquet/Delta)
- **Limite total do container:** 8 GB
- **Spill to disk:** habilitado — quando a memoria nao e suficiente, os dados intermediarios sao gravados em disco (volume `dremio-spill`)

Se precisar processar datasets muito grandes (dezenas de GB), pode aumentar os limites no `docker-compose.yml`:

```yaml
dremio:
  environment:
    - DREMIO_MAX_HEAP_MEMORY_SIZE_MB=8192     # 8 GB heap
    - DREMIO_MAX_DIRECT_MEMORY_SIZE_MB=4096   # 4 GB direct
  deploy:
    resources:
      limits:
        memory: 16g                            # 16 GB total
```

---

## Componentes da Stack (detalhes tecnicos)

### Spark Cluster

| Componente | Detalhes |
|---|---|
| Imagem base | `bitnami/spark:3.5.5` |
| Master | 1 instancia, porta 7077 (cluster) e 8080 (web UI mapeada para 8090) |
| Workers | 3 instancias, cada uma com 2 GB RAM e 2 cores |
| History Server | 1 instancia, porta 18080 |
| JARs extras | Delta Lake 3.2.0, Iceberg 1.9.1, Spark Measure 0.24.0, PostgreSQL JDBC |

O Spark opera em modo **standalone cluster**. O Jupyter atua como **driver** em modo **client**: ele envia comandos para o Master, que distribui para os Workers. Os Workers processam os dados e devolvem os resultados para o Jupyter.

### Jupyter Notebook (2 instancias)

| Detalhe | Valor |
|---|---|
| Imagem | Mesma do Spark (`Dockerfile.spark`) |
| Versao do Notebook | 6.5.7 (classico, estavel) |
| Instancias | 2 (`jupyter-1` na porta 8888, `jupyter-2` na porta 8889) |
| Autenticacao | Token (padrao: `spark123`, configuravel via variavel `JUPYTER_TOKEN`) |
| Usuario | `root` (necessario para permissoes no volume `work/`) |
| Diretorio de trabalho | `/opt/bitnami/spark/work` (mapeado para `./work/` local, compartilhado) |
| Limite Docker | 2 GB RAM, 1 CPU por instancia |
| Limite Spark | max 2 cores, 1 GB por executor, 512 MB driver |

### MinIO (Object Storage)

| Detalhe | Valor |
|---|---|
| Imagem | `minio/minio` (latest) |
| Modo | Cluster distribuido com 4 nodes (erasure coding) |
| API (S3) | Porta 9000 |
| Console Web | Porta 9001 |
| Credenciais | `minioadmin` / `minioadmin` |

O modo distribuido (`http://minio{1...4}/data`) garante redundancia: mesmo que 1 dos 4 nodes caia, os dados permanecem acessiveis.

### Dremio (Query Engine)

| Detalhe | Valor |
|---|---|
| Imagem | `dremio/dremio-oss` (open source) |
| Web UI | Porta 9047 |
| ODBC/JDBC | Porta 31010 |
| Arrow Flight | Porta 32010 |
| Heap Memory | 4 GB |
| Direct Memory | 2 GB |
| Container Limit | 8 GB |
| Spill to Disk | Habilitado |

---

## Dependencias Python

Instaladas automaticamente no build da imagem Spark:

| Pacote | Versao | Para que serve |
|---|---|---|
| pyspark | 3.5.1 | Apache Spark (API Python) |
| pandas | 2.2.2 | Manipulacao de dados em DataFrames Python |
| pyarrow | 16.1.0 | Formato columnar in-memory (Apache Arrow) |
| sparkmeasure | 0.24.0 | Metricas de performance do Spark |
| deltalake | 0.17.4 | Delta Lake (API Python nativa) |
| delta-spark | 3.2.0 | Delta Lake integrado ao Spark |
| jupyter | 1.0.0 | Metapackage do Jupyter |
| notebook | 6.5.7 | Jupyter Notebook classico |
| psycopg2-binary | 2.9.9 | Driver PostgreSQL para Python |
| sqlalchemy | 2.0.20 | ORM SQL para Python |

---

## Portas Utilizadas

| Porta | Servico | Protocolo |
|---|---|---|
| 7077 | Spark Master (comunicacao do cluster) | TCP |
| 8081 | Spark Worker 1 UI | HTTP |
| 8082 | Spark Worker 2 UI | HTTP |
| 8083 | Spark Worker 3 UI | HTTP |
| 8090 | Spark Master UI | HTTP |
| 8888 | Jupyter 1 | HTTP |
| 8889 | Jupyter 2 | HTTP |
| 9000 | MinIO API (compativel com S3) | HTTP |
| 9001 | MinIO Console (web UI) | HTTP |
| 9047 | Dremio Web UI | HTTP |
| 18080 | Spark History Server | HTTP |
| 31010 | Dremio ODBC/JDBC | TCP |
| 32010 | Dremio Arrow Flight | gRPC |
| 45678 | Dremio inter-node | TCP |

> Se alguma porta ja estiver em uso na sua maquina, altere o mapeamento no `docker-compose.yml`. Por exemplo, se a porta 8888 estiver ocupada, troque `"8888:8888"` para `"8889:8888"` e acesse via `http://localhost:8889`.

---

## Limites de Recursos

Todos os servicos possuem limites Docker (`deploy.resources.limits`) para evitar que a stack derrube a maquina:

| Servico | Memoria | CPUs | Spark cores.max | Spark executor.memory |
|---|---|---|---|---|
| spark-master | 1 GB | 1.0 | — | — |
| spark-worker-1/2/3 | 2.5 GB cada | 2.0 cada | — | — |
| spark-history | 512 MB | 0.5 | — | — |
| jupyter-1 | 2 GB | 1.0 | 2 | 1 GB |
| jupyter-2 | 2 GB | 1.0 | 2 | 1 GB |
| minio1-4 | sem limite | sem limite | — | — |
| dremio | 8 GB | sem limite | — | — |

**Como funciona a protecao:**

- **`spark.cores.max=2`** — cada aplicacao Spark (notebook) usa no maximo 2 dos 6 cores disponiveis no cluster. Isso permite 2 Jupiters rodando simultaneamente sem que um roube todos os recursos do outro.
- **`spark.executor.memory=1g`** — cada executor usa no maximo 1 GB. Com 2 cores por app, cada notebook usa ~2 GB do cluster (2 executors x 1 GB).
- **`spark.driver.memory=512m`** — o driver (Jupyter) usa no maximo 512 MB para processar resultados.
- **Docker limits** — mesmo que o Spark tente alocar mais, o Docker mata o container se ultrapassar o limite de memoria.

**Para ajustar os limites**, edite o `docker-compose.yml` (limites Docker) e o `spark-defaults.conf` (limites Spark).

---

## Persistencia de Dados

Todos os dados ficam em **bind mounts** (pastas locais na pasta `data/`), **nao** em Docker named volumes. Isso garante que:

- `docker compose down` **NAO** apaga dados
- `docker compose down -v` **NAO** apaga dados (so remove named volumes, que nao existem mais)
- Reiniciar o Docker Desktop **NAO** apaga dados
- Os dados sao **visiveis** no host e faceis de fazer backup

| Pasta | Conteudo |
|---|---|
| `data/minio1/` a `data/minio4/` | Buckets e objetos do MinIO (seus arquivos JSON, Parquet, Delta) |
| `data/dremio/` | Configuracoes do Dremio (conta admin, sources, metadados) |
| `data/dremio-spill/` | Area temporaria do Dremio para queries grandes |
| `data/spark-events/` | Historico de jobs Spark (visivel no History Server) |
| `work/` | Notebooks Jupyter e scripts Python |

> **Para apagar tudo e comecar do zero:**
> ```bash
> docker compose down
> rm -rf data/
> mkdir -p data/minio1 data/minio2 data/minio3 data/minio4 data/dremio data/dremio-spill data/spark-events
> chmod -R 777 data/dremio data/dremio-spill
> docker compose up -d
> ```

---

## Comandos Uteis

### Gerenciamento da stack

```bash
# Subir toda a stack (primeira vez ou apos alteracao no Dockerfile)
docker compose up -d --build

# Subir a stack (sem rebuild, mais rapido)
docker compose up -d

# Parar toda a stack (dados ficam preservados em data/)
docker compose down

# Ver status de todos os containers
docker compose ps

# Reiniciar um servico especifico
docker compose restart jupyter-1
docker compose restart jupyter-2
docker compose restart spark-master
```

### Logs e depuracao

```bash
# Ver logs de um servico (ultimas 50 linhas)
docker compose logs jupyter-1 --tail 50
docker compose logs jupyter-2 --tail 50
docker compose logs spark-master --tail 50
docker compose logs minio1 --tail 50
docker compose logs dremio --tail 50

# Acompanhar logs em tempo real
docker compose logs -f jupyter-1

# Entrar em um container para debug
docker exec -it jupyter-1 bash
docker exec -it spark-master bash
```

### Spark Submit (executar scripts sem Jupyter)

```bash
# Enviar um script Python para o cluster Spark
docker exec spark-master spark-submit /opt/bitnami/spark/work/meu_script.py
```

---

## Troubleshooting

### Container nao sobe / reinicia em loop

```bash
# Verificar logs do container
docker compose logs <servico> --tail 100

# Motivos comuns:
# - Porta ja em uso: altere o mapeamento de portas no docker-compose.yml
# - Memoria insuficiente: feche aplicacoes ou aumente a RAM do Docker Desktop
# - Imagem corrompida: docker compose build --no-cache
```

### "Initial job has not accepted any resources"

Este erro aparece no Jupyter quando o Spark nao consegue alocar executors nos workers.

**Causa:** O driver (Jupyter) e os workers nao conseguem se comunicar.

**Solucao:** O `jupyter-entrypoint.sh` configura automaticamente:
```
spark.driver.host        jupyter-1  (ou jupyter-2)
spark.driver.bindAddress 0.0.0.0
```

Se ainda ocorrer, verifique:
1. Os 3 workers estao Up: `docker compose ps`
2. Os workers aparecem no Spark Master UI (http://localhost:8090)
3. O container Jupyter esta na mesma rede: `spark-network`
4. Verifique se o entrypoint rodou: `docker exec jupyter-1 cat /tmp/spark-defaults.conf`

### Jupyter nao abre notebooks / Permission denied

**Causa:** O volume `./work/` foi criado com permissoes do host que nao combinam com o container.

**Solucao:** O Jupyter ja roda como `root` no docker-compose.yml (`user: root`).

Se mesmo assim der erro:
```bash
# No host, dar permissao total na pasta work
chmod -R 777 work/
```

### Dremio nao inicia / "path /opt/dremio/data is not writable"

**Causa:** A pasta `data/dremio/` nao tem permissao de escrita para o usuario interno do Dremio.

**Solucao:**
```bash
chmod -R 777 data/dremio data/dremio-spill
docker compose restart dremio
```

### MinIO nao inicia / "Formatting 1 zone"

**Causa:** Dados corrompidos ou configuracao inconsistente nas pastas do MinIO.

**Solucao:**
```bash
docker compose down
rm -rf data/minio1 data/minio2 data/minio3 data/minio4
mkdir -p data/minio1 data/minio2 data/minio3 data/minio4
docker compose up -d
```

> **Atencao:** Isso apaga todos os dados armazenados nos buckets!

### Dremio nao conecta no MinIO

Verifique a configuracao do source:
1. O **endpoint** deve ser `minio1:9000` (nome do container, nao `localhost`)
2. **Encrypt connection** deve estar desmarcado
3. **Enable compatibility mode** deve estar marcado
4. As connection properties `fs.s3a.endpoint` e `fs.s3a.path.style.access` devem estar configuradas

### Build demora muito / falha no pip install

```bash
# Forcar rebuild sem cache
docker compose build --no-cache

# Se o pip falhar por timeout, tente novamente (pode ser rede lenta)
docker compose up -d --build
```

### Logs muito verbosos no Jupyter

O nivel de log ja esta em `warn` no `log4j2.properties`. Se ainda quiser reduzir, adicione no notebook:

```python
spark.sparkContext.setLogLevel("ERROR")
```

---

## Fluxo Completo (passo a passo resumido)

1. **Criar pastas de dados:** `mkdir -p data/minio{1..4} data/dremio data/dremio-spill data/spark-events && chmod -R 777 data/dremio data/dremio-spill`
2. **Subir a stack:** `docker compose up -d --build`
3. **Esperar todos Up:** `docker compose ps`
4. **Criar buckets no MinIO:** `landing` e `processing` via http://localhost:9001 (login: `minioadmin`/`minioadmin`)
5. **Upload de dados:** Subir arquivos JSON para o bucket `landing`
6. **Processar no Jupyter:** Abrir http://localhost:8888/?token=spark123 (ou :8889 para o segundo), criar SparkSession, ler JSON, gravar Parquet/Delta
7. **Consultar no Dremio:** Abrir http://localhost:9047, conectar ao MinIO, promover pastas, consultar SQL
8. **Parar a stack:** `docker compose down` (dados preservados na pasta `data/`)
