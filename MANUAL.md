# Manual da Stack de Dados — stack-prev

**Data de atualizacao:** 2026-03-02
**Ambiente:** WSL2 (Ubuntu 20.04.6 LTS) | Kernel 6.6.87.2-microsoft-standard-WSL2
**Hardware:** Intel Core i9-13980HX | 32 vCPUs | 15.4 GB RAM | 1 TB disco

---

## Sumario

1. [Visao Geral da Stack](#1-visao-geral-da-stack)
2. [Estrutura de Arquivos](#2-estrutura-de-arquivos)
3. [Problemas Corrigidos e Solucoes](#3-problemas-corrigidos-e-solucoes)
4. [Como Operar a Stack](#4-como-operar-a-stack)
5. [Como Executar Scripts Spark](#5-como-executar-scripts-spark)
6. [Pipeline Landing Zone MinIO](#6-pipeline-landing-zone-minio)
7. [Configuracoes Importantes](#7-configuracoes-importantes)
8. [Seguranca — Acoes Pendentes](#8-seguranca--acoes-pendentes)
9. [Referencia de Comandos](#9-referencia-de-comandos)

---

## 1. Visao Geral da Stack

```
┌─────────────────────────────────────────────────────────┐
│                     Docker Network                       │
│                                                          │
│  ┌──────────────┐   ┌──────────────┐                    │
│  │ spark-master │   │ spark-history│                    │
│  │  :8090/:7077 │   │    :18080    │                    │
│  └──────┬───────┘   └──────────────┘                    │
│         │                                               │
│  ┌──────┴────────────────────────┐                      │
│  │  worker-1  worker-2  worker-3 │                      │
│  │  :8081     :8082     :8083    │                      │
│  └───────────────────────────────┘                      │
│                                                          │
│  ┌───────────┐  ┌───────────┐                           │
│  │ jupyter-1 │  │ jupyter-2 │                           │
│  │   :8888   │  │   :8889   │                           │
│  └───────────┘  └───────────┘                           │
│                                                          │
│  ┌────────────────────────────────────┐                  │
│  │  minio1:9000/9001  minio2/3/4:9000 │                  │
│  └────────────────────────────────────┘                  │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐                     │
│  │   dremio     │  │  portainer   │                     │
│  │    :9047     │  │   :9443      │                     │
│  └──────────────┘  └──────────────┘                     │
└─────────────────────────────────────────────────────────┘
```

| Container | Imagem | Porta(s) | Credenciais |
|-----------|--------|---------|-------------|
| spark-master | bitnami/spark:3.5.5 (custom) | 8090 (UI), 7077 (cluster) | — |
| spark-worker-1/2/3 | bitnami/spark:3.5.5 (custom) | 8081-8083 | — |
| spark-history | bitnami/spark:3.5.5 (custom) | 18080 | — |
| jupyter-1 | bitnami/spark:3.5.5 (custom) | 8888 | token: spark123 |
| jupyter-2 | bitnami/spark:3.5.5 (custom) | 8889 | token: spark123 |
| minio1 | minio/minio | 9000 (S3), 9001 (console) | minioadmin/minioadmin |
| minio2/3/4 | minio/minio | 9000 | minioadmin/minioadmin |
| dremio | dremio/dremio-oss | 9047 | configurar no primeiro acesso |
| portainer | portainer/portainer-ce | 9443 | configurar no primeiro acesso |

---

## 2. Estrutura de Arquivos

```
stack-prev/
├── Dockerfile.spark              # Imagem customizada Spark (Bitnami + fixes)
├── docker-compose.yml            # Definicao de todos os servicos
├── requirements.txt              # Dependencias Python (pyspark, pandas, etc.)
├── .gitignore                    # Exclui data/, dados-abertos/, .claude/
├── .dockerignore
├── MANUAL.md                     # Este arquivo
│
├── config/
│   └── spark/
│       ├── spark-defaults.conf   # Configuracoes Spark + S3A/MinIO
│       ├── log4j2.properties     # Niveis de log
│       └── jupyter-entrypoint.sh # Entrypoint dos containers Jupyter
│
├── data/                         # Dados persistentes (NAO versionado)
│   ├── spark-events/             # Event logs do Spark History Server
│   ├── minio1/2/3/4/             # Dados do cluster MinIO
│   ├── dremio/                   # Dados do Dremio
│   └── dremio-spill/
│
├── work/                         # Scripts e notebooks (montado em todos containers)
│   ├── test-read-s3-public.py    # Leitura dados publicos + gravacao MinIO landing
│   ├── landing-beneficios.py     # Pipeline landing zone (versao standalone)
│   ├── medallion-pipeline.ipynb  # Pipeline medallion (Bronze/Silver/Gold)
│   ├── medallion-parquet.ipynb   # Pipeline medallion com Parquet
│   ├── dados-abertos/            # Cache CSV (NAO versionado — 11 GB)
│   │   └── D.SDA.PDA.003.EMI.202601.csv
│   └── [outros notebooks...]
│
└── portainer/
    └── Dockerfile
```

---

## 3. Problemas Corrigidos e Solucoes

### Problema 1 — `basedir must be absolute: ?/.ivy2/local`

**Contexto:** Ao executar `spark-submit` via `docker exec`, o Ivy (gerenciador de dependencias do Spark) falhava ao tentar criar o diretorio `~/.ivy2/local`.

**Causa raiz:** O container Bitnami Spark usa `USER 1001` sem criar entrada correspondente no `/etc/passwd`. A JVM chama `getpwuid()` nativo para resolver o home directory e, sem entrada no passwd, retorna `?` (nao absoluto).

**Solucao — `Dockerfile.spark`:**
```dockerfile
RUN useradd --uid 1001 --gid 0 --home /opt/bitnami/spark --no-create-home --shell /bin/bash spark
```

**Solucao — `config/spark/spark-defaults.conf`:**
```
spark.jars.ivy                   /opt/bitnami/spark/.ivy2
spark.driver.extraJavaOptions    -Duser.home=/opt/bitnami/spark -Duser.name=spark
spark.executor.extraJavaOptions  -Duser.home=/opt/bitnami/spark -Duser.name=spark
```

---

### Problema 2 — `KerberosAuthException: NullPointerException: invalid null input: name`

**Contexto:** Mesmo ao corrigir o HOME, o Hadoop `UnixLoginModule` falhava ao inicializar o `SparkContext`.

**Causa raiz:** Mesma raiz do Problema 1 — `UnixLoginModule` usa `UnixPrincipal` que chama `getpwuid()` nativo. Sem entrada no `/etc/passwd` para uid=1001, o nome retorna null.

**Solucao:** Mesma do Problema 1 (`useradd` + `extraJavaOptions`).

---

### Problema 3 — `Permission denied` no diretorio de event log

**Contexto:** Mesmo com os fixes acima, o SparkContext falhava ao tentar escrever o arquivo `.inprogress` no diretorio de event log.

**Causa raiz:** O diretorio `./data/spark-events/` no host tinha permissao `755` (owner: nobru/uid=1000). O spark user dentro do container e uid=1001, sem permissao de escrita.

**Solucao (host):**
```bash
chmod 777 /home/nobru/documentos/stack-prev/data/spark-events/
```

---

### Problema 4 — Script nao encontrado nos containers

**Contexto:** `spark-submit` com caminho `/home/nobru/documentos/...` falhava com `No such file or directory`.

**Causa raiz:** O diretorio `./work` do host nao estava montado no container `spark-master`.

**Solucao — `docker-compose.yml` (spark-master):**
```yaml
volumes:
  - ./work:/opt/bitnami/spark/work
```
**Caminho correto para scripts:** `/opt/bitnami/spark/work/SEU_SCRIPT.py`

---

### Problema 5 — Workers nao encontravam o CSV

**Contexto:** Ao rodar em modo cluster (`--master spark://spark-master:7077`), os workers falhavam com `SparkFileNotFoundException`.

**Causa raiz:** O CSV estava no host em `./work/dados-abertos/` mas os workers nao tinham o volume `./work` montado. Cada worker tentava ler o arquivo localmente e nao encontrava.

**Solucao — `docker-compose.yml` (todos os workers):**
```yaml
volumes:
  - ./work:/opt/bitnami/spark/work
```

---

## 4. Como Operar a Stack

### Iniciar tudo
```bash
cd /home/nobru/documentos/stack-prev
docker compose up -d
```

### Parar tudo
```bash
docker compose down
```

### Ver status dos containers
```bash
docker compose ps
# ou com uso de recursos:
docker stats --no-stream
```

### Rebuild apos mudancas no Dockerfile ou requirements.txt
```bash
docker compose build
docker compose up -d
```

### Restart de um servico especifico
```bash
docker compose restart spark-master
docker compose up -d spark-worker-1  # recria se mudou config
```

### Ver logs de um container
```bash
docker logs spark-master --tail 50 -f
docker logs jupyter-1 --tail 50
```

### Acessar shell dentro de um container
```bash
docker exec -it spark-master bash
docker exec -it jupyter-1 bash
```

### Limpar imagens e volumes nao utilizados (~50 GB recuperaveis)
```bash
# Ver o que sera removido:
docker system df
docker images -f "dangling=true"

# Limpeza segura (so orfas + cache de build):
docker image prune -f
docker builder prune -f
docker volume prune -f

# Limpeza total (CUIDADO: remove tudo nao em uso):
docker system prune -a -f --volumes
```

---

## 5. Como Executar Scripts Spark

### Comando padrao (funciona sem flags extras apos os fixes)
```bash
docker exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/work/SEU_SCRIPT.py
```

### Coloque seus scripts em `./work/`
Os scripts ficam disponiveis automaticamente em `/opt/bitnami/spark/work/` dentro de todos os containers Spark e Jupyter.

### Historico de execucoes
Acesse o Spark History Server em: http://localhost:18080

### Parametros uteis no spark-submit
```bash
# Aumentar memoria do driver para arquivos grandes:
--conf spark.driver.memory=4g

# Aumentar numero de particoes para shuffle:
--conf spark.sql.shuffle.partitions=50

# Aumentar memoria dos executores:
--conf spark.executor.memory=2g
```

---

## 6. Pipeline Landing Zone MinIO

### Scripts disponiveis

| Script | Descricao |
|--------|-----------|
| `work/test-read-s3-public.py` | Download CSV publico + analise + grava MinIO |
| `work/landing-beneficios.py` | Pipeline landing zone (versao standalone completa) |

### Fluxo do pipeline

```
Portal Dados Abertos (S3 publico)
         |
         | urllib.request (download ZIP ~549 MB)
         v
./work/dados-abertos/  (cache local 11 GB CSV)
         |
         | spark.read.csv (inferSchema, sep=;, ISO-8859-1)
         v
  DataFrame Spark
  41.572.553 linhas x 14 colunas
         |
         | df.write.mode("overwrite").parquet(...)
         v
  MinIO: s3a://landing/pda/beneficios-emitidos/202601/
```

### Executar
```bash
docker exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/work/test-read-s3-public.py
```

### Acessar dados no MinIO via Spark
```python
df = spark.read.parquet("s3a://landing/pda/beneficios-emitidos/202601/")
df.count()  # 41.572.553
```

### Console MinIO
http://localhost:9001 — usuario: `minioadmin` / senha: `minioadmin`

### Estrutura de buckets recomendada
```
landing/    ← dados raw (Parquet, sem transformacao)
  pda/
    beneficios-emitidos/
      202601/
bronze/     ← dados limpos, tipados
silver/     ← dados agregados/enriquecidos
gold/       ← dados prontos para consumo
```

---

## 7. Configuracoes Importantes

### spark-defaults.conf
```properties
spark.master                     spark://spark-master:7077
spark.eventLog.enabled           true
spark.eventLog.dir               file:/opt/bitnami/spark/logs/events
spark.history.fs.logDirectory    file:/opt/bitnami/spark/logs/events

# Limites por aplicacao
spark.driver.memory              512m
spark.cores.max                  2
spark.executor.memory            1g

# Fix JVM para uid=1001 sem /etc/passwd (OBRIGATORIO)
spark.jars.ivy                   /opt/bitnami/spark/.ivy2
spark.driver.extraJavaOptions    -Duser.home=/opt/bitnami/spark -Duser.name=spark
spark.executor.extraJavaOptions  -Duser.home=/opt/bitnami/spark -Duser.name=spark

# MinIO S3A
spark.hadoop.fs.s3a.endpoint              http://minio1:9000
spark.hadoop.fs.s3a.access.key            minioadmin
spark.hadoop.fs.s3a.secret.key            minioadmin
spark.hadoop.fs.s3a.path.style.access     true
spark.hadoop.fs.s3a.impl                  org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.connection.ssl.enabled false
```

### Volumes montados por container

| Container | Host | Container |
|-----------|------|-----------|
| spark-master | `./work` | `/opt/bitnami/spark/work` |
| spark-master | `./data/spark-events` | `/opt/bitnami/spark/logs/events` |
| spark-worker-1/2/3 | `./work` | `/opt/bitnami/spark/work` |
| spark-worker-1/2/3 | `./data/spark-events` | `/opt/bitnami/spark/logs/events` |
| jupyter-1/2 | `./work` | `/opt/bitnami/spark/work` |
| jupyter-1/2 | `./data/spark-events` | `/opt/bitnami/spark/logs/events` |

### Limites de recursos

| Container | CPU | RAM |
|-----------|-----|-----|
| spark-master | 1 core | 1 GB |
| spark-worker-1/2/3 | 2 cores | 2.5 GB |
| spark-history | 0.5 core | 512 MB |
| jupyter-1/2 | 1 core | 2 GB |
| dremio | sem limite | 8 GB |

---

## 8. Seguranca — Acoes Pendentes

### Medio prazo

**1. Trocar token Jupyter (token fraco em texto plano)**
```bash
# Gerar token forte:
TOKEN=$(openssl rand -hex 32)
# Editar docker-compose.yml e substituir spark123 pelo token gerado
# Recriar os containers:
docker compose up -d jupyter-1 jupyter-2
```

**2. Corrigir PermitRootLogin no SSH (inativo, mas ja corrigir)**
```bash
sudo sed -i 's/PermitRootLogin yes/PermitRootLogin no/' /etc/ssh/sshd_config
sudo sshd -t  # validar sem reiniciar
```

**3. Proteger token Kubernetes**
```bash
chmod 600 /home/nobru/portainer-token.txt
```

### Limpeza recomendada

**Pacotes desatualizados (8 pendentes)**
```bash
sudo apt update && sudo apt upgrade -y
```

**Servicos desnecessarios no WSL2**
```bash
sudo systemctl disable --now wpa_supplicant.service
sudo systemctl disable --now ModemManager.service
```

**Ubuntu 20.04 fora de suporte (desde abril/2025)**
```bash
# Backup antes de migrar:
# No PowerShell do Windows:
wsl --export Ubuntu-20.04 backup-ubuntu-20.04.tar
# Depois migrar para Ubuntu 22.04 LTS
```

---

## 9. Referencia de Comandos

### Docker

```bash
# Status geral
docker compose ps
docker stats --no-stream
docker system df

# Gerenciar containers
docker compose up -d                          # sobe tudo
docker compose down                           # para tudo
docker compose build                          # rebuild imagens
docker compose up -d spark-master             # sobe/recria um servico
docker compose restart spark-history         # restart sem recriar

# Logs
docker logs spark-master -f --tail 100
docker logs jupyter-1 --tail 50

# Executar comandos
docker exec spark-master bash
docker exec -e VAR=valor spark-master comando

# Copiar arquivos para container
docker cp ./arquivo.py spark-master:/tmp/

# Adicionar usuario em container ativo (nao persiste apos restart)
docker exec -u root spark-master sh -c 'echo "spark:x:1001:0:spark:/tmp:/bin/sh" >> /etc/passwd'

# Permissoes no host para volumes montados
chmod 777 ./data/spark-events/
```

### Spark

```bash
# Submeter job no cluster
docker exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/work/SCRIPT.py

# Ver versao
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --version

# Ver aplicacoes ativas
curl -s http://localhost:8090/api/v1/applications | python3 -m json.tool
```

### MinIO via mc (MinIO Client)

```bash
# Configurar alias (se mc instalado no host)
mc alias set local http://localhost:9000 minioadmin minioadmin

# Listar buckets
mc ls local/

# Listar arquivos em bucket
mc ls local/landing/

# Verificar tamanho
mc du local/landing/

# Criar bucket
mc mb local/bronze
```

### Git

```bash
# Ver historico
git log --oneline

# Ver o que mudou
git diff
git status

# Commitar
git add ARQUIVO
git commit -m "descricao"

# Nao versionar dados grandes (ja no .gitignore):
# work/dados-abertos/  (CSV 11 GB)
# data/                (dados dos containers)
# .claude/             (arquivos internos Claude)
```

### Diagnostico

```bash
# Verificar usuario dentro do container
docker exec spark-master id
docker exec spark-master sh -c 'getent passwd spark'

# Verificar variaveis de ambiente
docker exec spark-master env | grep -E "HOME|USER|SPARK|JAVA"

# Verificar montagem de volumes
docker inspect spark-master | python3 -c "
import json,sys
data = json.load(sys.stdin)
mounts = data[0]['Mounts']
for m in mounts: print(m['Source'], '->', m['Destination'])
"

# Testar conectividade MinIO de dentro do Spark
docker exec spark-master \
  curl -s http://minio1:9000/minio/health/live && echo " MinIO OK"

# Ver event logs disponiveis
ls -lh ./data/spark-events/
```

---

*Documento gerado em 2026-03-02 | Stack: Spark 3.5.5 + MinIO + Dremio + Jupyter*
