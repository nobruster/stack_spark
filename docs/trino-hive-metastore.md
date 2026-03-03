# Manual Completo: Trino + Hive Metastore + PostgreSQL

## Índice

1. [Visão Geral da Arquitetura](#1-visão-geral-da-arquitetura)
2. [Por que cada componente existe](#2-por-que-cada-componente-existe)
3. [Estrutura de Arquivos](#3-estrutura-de-arquivos)
4. [PostgreSQL — Backend do Metastore](#4-postgresql--backend-do-metastore)
5. [Hive Metastore — Catálogo de Metadados](#5-hive-metastore--catálogo-de-metadados)
6. [Trino — Query Engine](#6-trino--query-engine)
7. [Catálogo Delta Lake](#7-catálogo-delta-lake)
8. [Procedimento de Inicialização](#8-procedimento-de-inicialização)
9. [Queries de Referência](#9-queries-de-referência)
10. [Como Adicionar Novas Tabelas](#10-como-adicionar-novas-tabelas)
11. [Troubleshooting](#11-troubleshooting)
12. [Decisões Técnicas e Armadilhas](#12-decisões-técnicas-e-armadilhas)

---

## 1. Visão Geral da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│                          STACK DE DADOS                             │
│                                                                     │
│  ┌─────────────┐    ┌─────────────┐    ┌──────────────────────┐   │
│  │   Jupyter   │    │    Spark    │    │       Trino          │   │
│  │  (PySpark)  │    │  (cluster)  │    │  coordinator + 2w    │   │
│  │  :8888/9   │    │  :8080      │    │  Web UI :8086         │   │
│  └──────┬──────┘    └──────┬──────┘    └──────────┬───────────┘   │
│         │                  │                       │               │
│         │         ESCRITA (Delta Lake)             │ LEITURA (SQL) │
│         │                  │                       │               │
│         └──────────────────┼───────────────────────┘               │
│                            │                                        │
│                     ┌──────▼──────────────────────┐                │
│                     │         MinIO (S3)           │                │
│                     │  s3://bronze/  s3://prata/   │                │
│                     │  s3://ouro/    s3://landing/ │                │
│                     └─────────────────────────────┘                │
│                                                                     │
│  ┌──────────────────────────────────────────────┐                  │
│  │  Hive Metastore (HMS)   :9083 thrift         │                  │
│  │  "onde ficam os metadados das tabelas Delta" │                  │
│  └───────────────────────┬──────────────────────┘                  │
│                          │ JDBC                                     │
│               ┌──────────▼──────────┐                              │
│               │  PostgreSQL :5432   │                              │
│               │  DB: metastore      │                              │
│               └─────────────────────┘                              │
└─────────────────────────────────────────────────────────────────────┘
```

### Fluxo de dados

```
dados brutos (CSV/Parquet)
        │
        ▼ Spark/PySpark escreve
   s3://bronze/         ←── Delta Lake (formato de arquivo)
        │
        ▼ Spark/PySpark transforma
   s3://prata/          ←── Delta Lake (41.5M linhas, Parquet + _delta_log/)
        │
        ▼ Spark/PySpark agrega
   s3://ouro/           ←── Delta Lake (tabelas fat_ e kpis)
        │
        ▼ Trino lê diretamente via S3 nativo
   SELECT * FROM delta.ouro.kpis_nacionais
```

### Papel do Hive Metastore neste fluxo

O Trino **não** lê diretamente o S3 sem saber o que tem lá. Ele precisa de um
**catálogo de metadados** que informe:

- Quais schemas/bancos existem
- Quais tabelas existem dentro de cada schema
- Onde no S3 está cada tabela (`table_location`)
- O schema (colunas, tipos) de cada tabela — lido do Delta Log no S3

O HMS armazena esse catálogo no PostgreSQL e o expõe via protocolo **Thrift** na
porta 9083.

---

## 2. Por que cada componente existe

| Componente | Função | Substituível por? |
|---|---|---|
| **PostgreSQL** | Banco relacional que persiste o schema do HMS | MySQL, MariaDB (HMS suporta) |
| **Hive Metastore** | Servidor Thrift que expõe metadados para Trino | Glue (AWS), Polaris (cloud) |
| **Trino coordinator** | Recebe queries SQL, planeja, distribui | — |
| **Trino workers (x2)** | Executam fragmentos do plano em paralelo | Adicionar mais para escalar |

### Por que não usar Dremio para isso?

O Dremio também conecta ao MinIO e lê Delta Lake. A diferença:

- **Dremio**: interface visual, catálogo próprio, excelente para exploração ad-hoc
- **Trino**: SQL puro ANSI, integra com BI tools (Metabase, Superset), APIs JDBC/ODBC,
  mais leve para automação e pipelines

Ambos coexistem na stack sem conflito.

---

## 3. Estrutura de Arquivos

```
stack-prev/
│
├── Dockerfile.hive-metastore          # Imagem customizada do HMS
│
├── config/
│   ├── hive-metastore/
│   │   ├── hive-site.xml              # Config S3A: MinIO como filesystem S3
│   │   └── entrypoint.sh             # Script de boot customizado do HMS
│   │
│   └── trino/
│       ├── catalog/
│       │   └── delta.properties       # Catálogo Delta Lake (MinIO + HMS)
│       │
│       ├── coordinator/               # Config exclusiva do coordinator
│       │   ├── config.properties      # Porta, memória, discovery URI
│       │   ├── jvm.config             # Heap Java (1500M)
│       │   ├── node.properties        # ID do nó e diretório de dados
│       │   └── log.properties         # Nível de log (INFO)
│       │
│       └── worker/                    # Config dos 2 workers
│           ├── config.properties      # coordinator=false, memória por nó
│           ├── jvm.config             # Heap Java (2G)
│           ├── node.properties        # node.id=${ENV:HOSTNAME} (único por container)
│           └── log.properties         # Nível de log (INFO)
│
└── work/
    └── init-trino.sql                 # Script idempotente de registro de tabelas
```

---

## 4. PostgreSQL — Backend do Metastore

### Configuração no docker-compose.yml

```yaml
postgres-metastore:
  image: postgres:15-alpine
  container_name: postgres-metastore
  environment:
    POSTGRES_DB: metastore       # banco criado automaticamente na 1a execução
    POSTGRES_USER: hive          # usuário proprietário do banco
    POSTGRES_PASSWORD: hive      # senha (credenciais internas, não expostas)
  deploy:
    resources:
      limits:
        memory: 256m             # leve: só armazena metadados, não dados reais
        cpus: '0.5'
  volumes:
    - postgres_data:/var/lib/postgresql/data   # volume nomeado = persistência
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U hive -d metastore"]
    interval: 5s
    timeout: 5s
    retries: 10
  networks:
    - spark-network
```

### Por que `postgres_data` é um volume nomeado (não bind mount)?

Um **volume nomeado** (`postgres_data: driver: local`) é gerenciado pelo Docker e
persiste mesmo quando o container é removido e recriado. O HMS inicializa o schema
do PostgreSQL **apenas uma vez** (na primeira execução). Se usarmos um bind mount
em `./data/postgres/`, o Docker inicializa o PostgreSQL normalmente. Ambos
funcionam, mas o volume nomeado é mais portável.

### O que o HMS cria no PostgreSQL?

Na primeira execução, o `schematool -initSchema` cria ~70 tabelas no banco
`metastore`. As mais importantes:

| Tabela HMS | O que armazena |
|---|---|
| `DBS` | Schemas (databases): ouro, prata, bronze |
| `TBLS` | Tabelas registradas em cada schema |
| `COLUMNS_V2` | Colunas de cada tabela (tipo, nome, posição) |
| `SDS` | Storage Descriptor: localização S3 e formato de cada tabela |
| `PARTITIONS` | Partições de tabelas particionadas |

### Como verificar o PostgreSQL

```bash
# Entrar no psql dentro do container
docker exec -it postgres-metastore psql -U hive -d metastore

# Ver schemas HMS registrados
SELECT DB_ID, NAME, DB_LOCATION_URI FROM DBS;

# Ver tabelas registradas
SELECT t.TBL_NAME, d.NAME AS schema, s.LOCATION
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
JOIN SDS s ON t.SD_ID = s.SD_ID;

# Sair
\q
```

---

## 5. Hive Metastore — Catálogo de Metadados

### Por que precisamos de um Dockerfile customizado?

A imagem `apache/hive:3.1.3` não vem com:

1. **Driver JDBC do PostgreSQL** — necessário para conectar ao `postgres-metastore`
2. **JARs S3A** — necessários para o HMS aceitar `s3://` como localização de tabela
3. **Entrypoint idempotente** — a imagem original falha em restart se o schema já existe

### Dockerfile.hive-metastore explicado linha por linha

```dockerfile
FROM apache/hive:3.1.3
# Imagem oficial Apache Hive com Hadoop embutido (versão standalone)
# Contém: /opt/hive/ + /opt/hadoop/ com os JARs Hadoop

USER root
# Precisamos de root para instalar bibliotecas e mudar permissões

# ── 1. Driver JDBC do PostgreSQL ─────────────────────────────────
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    /opt/hive/lib/postgresql-42.7.3.jar
# ADD baixa da URL e adiciona ao filesystem da imagem durante o build
# /opt/hive/lib/ está sempre no CLASSPATH do Hive (todos JARs são carregados)
# Nota: curl e wget NÃO existem nessa imagem — ADD é a única opção

# ── 2. JARs S3A (já existem na imagem, só precisam estar no classpath certo) ─
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.1.0.jar \
       /opt/hive/lib/hadoop-aws-3.1.0.jar && \
    cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.271.jar \
       /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar && \
    chmod 644 /opt/hive/lib/hadoop-aws-3.1.0.jar \
              /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar \
              /opt/hive/lib/postgresql-42.7.3.jar
# Os JARs S3A EXISTEM em /opt/hadoop/tools/lib/ mas NÃO estão no CLASSPATH padrão
# Copiando para /opt/hive/lib/ garantimos que sejam carregados automaticamente
# hadoop-aws-3.1.0.jar    → implementa S3AFileSystem (org.apache.hadoop.fs.s3a.*)
# aws-java-sdk-bundle     → SDK AWS para comunicação HTTP com MinIO

# ── 3. Entrypoint customizado ─────────────────────────────────────
COPY config/hive-metastore/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && \
    chown hive:hive /opt/hive/conf/
# chown necessário porque /opt/hive/conf/ contém arquivos root:root
# (inclusive o hive-site.xml da imagem base)
# Sem chown, o usuário hive não consegue sobrescrever esses arquivos

USER hive
# Volta ao usuário não-root para segurança

ENTRYPOINT ["/entrypoint.sh"]
# Substitui o entrypoint original da imagem (que usava gunicorn/Python)
```

### entrypoint.sh explicado

```bash
#!/bin/bash
set -e   # Aborta em qualquer erro não tratado

# ── PASSO 1: Copiar hive-site.xml do volume para /opt/hive/conf/ ──
# Problema resolvido:
#   A imagem apache/hive:3.1.3 suporta HIVE_CUSTOM_CONF_DIR no entrypoint
#   ORIGINAL. Como substituímos o entrypoint, essa feature é bypassed.
#   Solução: copiamos manualmente os XMLs do volume para o conf dir.
#
# Por que rm -f antes do cp?
#   /opt/hive/conf/hive-site.xml já existe na imagem base (owner: root:root)
#   O usuário hive NÃO pode sobrescrever arquivos de root (mesmo sendo dono do dir)
#   Mas pode DELETAR do diretório (pois é dono do diretório)
#   Então: rm -f deleta o arquivo root, depois cp cria novo arquivo como hive:hive
if [ -n "$HIVE_CUSTOM_CONF_DIR" ] && [ -d "$HIVE_CUSTOM_CONF_DIR" ]; then
    echo "=== Aplicando conf de $HIVE_CUSTOM_CONF_DIR → /opt/hive/conf/ ==="
    for f in "$HIVE_CUSTOM_CONF_DIR"/*.xml; do
        if [ -f "$f" ]; then
            rm -f "/opt/hive/conf/$(basename "$f")"    # deleta versão root
            cp "$f" /opt/hive/conf/ && echo "    Copiado: $(basename $f)"
        fi
    done
fi

# ── PASSO 2: Configurar memória JVM ──────────────────────────────
export HADOOP_CLIENT_OPTS=" -Xmx1G ${SERVICE_OPTS}"
# SERVICE_OPTS vem do docker-compose e contém as flags JDBC:
#   -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
#   -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres-metastore:5432/metastore
#   -Djavax.jdo.option.ConnectionUserName=hive
#   -Djavax.jdo.option.ConnectionPassword=hive

# ── PASSO 3: Inicializar schema (idempotente) ─────────────────────
# Problema resolvido:
#   schematool -initSchema falha em restart com erro:
#   "relation BUCKETING_COLS already exists"
#   Solução: verificar se schema já existe antes de inicializar
if /opt/hive/bin/schematool -dbType postgres -info 2>/dev/null | grep -q "Hive distribution version"; then
    echo "    Schema já inicializado — pulando initSchema"
else
    echo "    Inicializando schema HMS no PostgreSQL..."
    /opt/hive/bin/schematool -dbType postgres -initSchema
fi

# ── PASSO 4: Iniciar o servidor HMS ──────────────────────────────
exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service metastore
# --skiphadoopversion: não verifica compatibilidade de versão Hadoop (evita warning)
# --skiphbasecp:       não adiciona HBase ao classpath (não usamos HBase)
# --service metastore: inicia o servidor Thrift na porta 9083
```

### hive-site.xml explicado

```xml
<!-- Arquivo: config/hive-metastore/hive-site.xml -->
<!-- Montado em: /hive_custom_conf/ no container -->
<!-- Copiado para: /opt/hive/conf/hive-site.xml pelo entrypoint -->

<!-- ── Filesystem S3 via protocolo s3:// ─────────────────────── -->
<property>
    <name>fs.s3.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    <!-- Mapeia o scheme "s3://" para S3AFileSystem (implementação MinIO-compatível) -->
</property>
<property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    <!-- Mesmo para "s3a://" -->
</property>
<property>
    <name>fs.AbstractFileSystem.s3.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3A</value>
    <!-- Nova API de filesystem do Hadoop (AbstractFileSystem) — necessária -->
</property>
<property>
    <name>fs.AbstractFileSystem.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3A</value>
</property>

<!-- ── Credenciais MinIO ─────────────────────────────────────── -->
<property>
    <name>fs.s3a.endpoint</name>
    <value>http://minio1:9000</value>   <!-- minio1 = hostname do container MinIO -->
</property>
<property>
    <name>fs.s3a.access.key</name>
    <value>minioadmin</value>
</property>
<property>
    <name>fs.s3a.secret.key</name>
    <value>minioadmin</value>
</property>
<property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
    <!-- OBRIGATÓRIO para MinIO: usa path-style (http://host:port/bucket/key)
         em vez de virtual-hosted-style (http://bucket.host:port/key) -->
</property>
<property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value>   <!-- MinIO sem TLS na rede interna -->
</property>

<!-- ── Permissões ───────────────────────────────────────────── -->
<property>
    <name>hive.metastore.pre.event.listeners</name>
    <value></value>
    <!-- Desabilita event listeners que validariam permissões no HMS.
         O controle de acesso é feito pelo Trino, não pelo HMS. -->
</property>
```

### Configuração do HMS no docker-compose.yml

```yaml
hive-metastore:
  build:
    context: .
    dockerfile: Dockerfile.hive-metastore   # imagem customizada
  container_name: hive-metastore
  environment:
    SERVICE_NAME: metastore                 # diz ao Hive qual serviço iniciar
    DB_DRIVER: postgres                     # tipo de RDBMS do backend

    # Estas flags JDBC são passadas via HADOOP_CLIENT_OPTS no entrypoint
    SERVICE_OPTS: >-
      -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
      -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres-metastore:5432/metastore
      -Djavax.jdo.option.ConnectionUserName=hive
      -Djavax.jdo.option.ConnectionPassword=hive

    HIVE_CUSTOM_CONF_DIR: /hive_custom_conf   # dir do volume com hive-site.xml

  deploy:
    resources:
      limits:
        memory: 768m    # JVM com -Xmx1G, mas container limitado a 768m
        cpus: '1.0'     # só precisa de 1 CPU — operações de metadados são leves

  ports:
    - "9083:9083"       # protocolo Thrift (binário, não HTTP)

  volumes:
    - ./config/hive-metastore:/hive_custom_conf   # hive-site.xml disponível no container

  depends_on:
    postgres-metastore:
      condition: service_healthy   # só sobe DEPOIS do PG estar pronto

  restart: on-failure   # reinicia se falhar (ex: PG ainda não pronto)

  healthcheck:
    # bash /dev/tcp: técnica para testar porta TCP sem netcat/nc/ss (não disponíveis na imagem)
    # exec 3<>/dev/tcp/localhost/9083 abre conexão TCP ao HMS
    # Se bem-sucedido: exit 0 (healthy)
    # Se falhar: exit 1 (unhealthy)
    test: ["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/localhost/9083' 2>/dev/null && echo ok || exit 1"]
    interval: 10s
    timeout: 5s
    retries: 20          # até 200s de espera (HMS demora ~60-90s para inicializar)
    start_period: 90s    # não conta retries nos primeiros 90s
```

---

## 6. Trino — Query Engine

### Conceitos fundamentais

**Coordinator**: nó que recebe a query SQL, gera o plano de execução distribuído e
retorna o resultado para o cliente. Não executa fragmentos de query diretamente.

**Workers**: nós que executam os fragmentos do plano em paralelo. Lêem os dados do
S3, aplicam filtros, fazem joins e retornam fragmentos de resultado ao coordinator.

**Catálogo**: abstração de um conjunto de dados acessível via Trino. O catálogo
`delta` usa o conector Delta Lake apontando para o HMS e o MinIO.

### Topologia no docker-compose.yml

```
trino-coordinator   ← porta 8086 (Web UI) / 8080 (inter-nó)
      │
      ├── trino-worker-1   (sem porta pública)
      └── trino-worker-2   (sem porta pública)
```

Todos na rede `spark-network` (bridge), comunicando-se diretamente por hostname Docker.

### config.properties — Coordinator

```properties
coordinator=true
# Este nó É o coordinator (não executa fragmentos de query)

node-scheduler.include-coordinator=false
# Não alocar fragmentos de execução para o coordinator
# (reserva CPU/memória apenas para planejamento e coordenação)

http-server.http.port=8080
# Porta interna de comunicação entre nós (coordinator ↔ workers)
# A porta pública 8086 é mapeada para esta via docker-compose

discovery.uri=http://trino-coordinator:8080
# URI onde os workers registram presença
# Coordinator descobre workers via este endpoint

query.max-memory=2GB
# Memória total disponível para uma única query no CLUSTER inteiro
# Se uma query precisar de mais, ela falha com OOM query-level

query.max-memory-per-node=800MB
# Memória máxima por nó para uma query
# Coordinator tem heap -Xmx1500M → 800MB é seguro (sobra 700M para overhead)
```

### jvm.config — Coordinator

```
-server                            # modo server JVM (otimizado para throughput)
-Xmx1500M                          # heap máximo 1.5GB
-XX:InitialRAMPercentage=80        # heap inicial = 80% do Xmx
-XX:MaxRAMPercentage=80            # heap máximo = 80% do Xmx (redundante com Xmx, mas seguro)
-XX:+ExplicitGCInvokesConcurrent   # GC explícito não para o mundo
-XX:+HeapDumpOnOutOfMemoryError    # gera heap dump em OOM (diagnóstico)
-XX:+ExitOnOutOfMemoryError        # mata o processo em OOM (reinicia limpo)
-Djdk.attach.allowAttachSelf=true  # permite profiling do próprio processo
-Djdk.nio.maxCachedBufferSize=2000000  # limita caching de buffers NIO (evita leak)
-XX:ReservedCodeCacheSize=256M     # cache de JIT compilado
```

**ATENÇÃO — Compatibilidade Java 21**: Trino 435 usa Java 21. Flags removidas
em Java 21 que NÃO devem aparecer no jvm.config:
- `-XX:-UseBiasedLocking` (removido no Java 15)
- `-XX:G1HeapRegionSize=32M` (gerenciado automaticamente)
- `-XX:+UseGCOverheadLimit` (removido)

### config.properties — Workers

```properties
coordinator=false
# Este nó é um worker (executa fragmentos de query)

http-server.http.port=8080
# Porta de comunicação interno (mesma do coordinator)

discovery.uri=http://trino-coordinator:8080
# Workers se registram no coordinator via este URI na inicialização

query.max-memory=2GB
# Deve ser igual ao coordinator (configuração global)

query.max-memory-per-node=1GB
# Workers têm heap -Xmx2G → 1GB é seguro (sobra 1G para overhead)
# Regra: max-memory-per-node + 30% headroom < Xmx
# Verificação: 1024MB + 0.3*2048MB = 1024 + 614 = 1638MB < 2048MB ✓
```

### jvm.config — Workers

```
-Xmx2G   # workers têm mais memória (executam os dados reais)
# ... restante igual ao coordinator
```

### node.properties

**Coordinator:**
```properties
node.environment=production     # ambiente (development/test/production)
node.id=trino-coordinator       # ID único fixo para o coordinator
node.data-dir=/data/trino       # diretório para logs e dados temporários
```

**Workers:**
```properties
node.environment=production
node.id=${ENV:HOSTNAME}         # cada worker tem hostname Docker único (gerado automaticamente)
                                # ex: "3a4fc8e23dac", "c5a30bb513d8"
node.data-dir=/data/trino
```

O `${ENV:HOSTNAME}` é expandido pelo Trino na inicialização usando a variável de
ambiente `HOSTNAME` do container Docker — garantindo ID único sem configuração manual.

### log.properties

```properties
io.trino=INFO
# Nível INFO: queries, conexões, erros. Adequado para produção.
# Para debug de S3/Delta: adicionar
#   io.trino.plugin.deltalake=DEBUG
#   io.trino.filesystem=DEBUG
```

### Configuração dos workers no docker-compose.yml

```yaml
trino-worker-1:
  image: trinodb/trino:435     # imagem oficial, sem customização
  container_name: trino-worker-1
  deploy:
    resources:
      limits:
        memory: 2.5g           # 2GB heap + 500MB overhead JVM + buffers OS
        cpus: '2.0'
  volumes:
    - ./config/trino/worker:/etc/trino          # config.properties, jvm.config, etc.
    - ./config/trino/catalog:/etc/trino/catalog  # delta.properties
    # core-site.xml: não mais necessário (fs.native-s3.enabled=true dispensa Hadoop S3A)
  depends_on:
    - trino-coordinator        # garante que coordinator está up antes
  restart: on-failure
  networks:
    - spark-network
```

---

## 7. Catálogo Delta Lake

### config/trino/catalog/delta.properties

```properties
connector.name=delta_lake
# Ativa o conector Delta Lake do Trino
# Carregado automaticamente de /etc/trino/catalog/

# ── Hive Metastore ───────────────────────────────────────────────
hive.metastore.uri=thrift://hive-metastore:9083
# Endereço do HMS. Trino usa este endpoint para:
#   - Criar/listar schemas
#   - Registrar e consultar tabelas
#   - Obter localização S3 de cada tabela

hive.metastore-timeout=30s
# Timeout para operações com o HMS (criação de schema pode demorar)

# ── Filesystem S3 Nativo do Trino ────────────────────────────────
fs.native-s3.enabled=true
# Usa o filesystem S3 nativo do Trino (não Hadoop S3A)
# IMPORTANTE: Trino 435 NÃO tem hadoop-aws.jar no classpath
# Se tentarmos usar S3A, recebemos: ClassNotFoundException: S3AFileSystem
# Com native-s3: Trino usa seu próprio cliente S3 (sem dependência Hadoop)

s3.endpoint=http://minio1:9000    # endpoint MinIO (container na mesma rede)
s3.path-style-access=true         # OBRIGATÓRIO para MinIO
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
s3.region=us-east-1               # MinIO aceita qualquer região; us-east-1 é o default

# ── Delta Lake ───────────────────────────────────────────────────
delta.enable-non-concurrent-writes=true
# Permite escritas sem lock distribuído
# NECESSÁRIO: nosso MinIO não tem DynamoDB para lock distribuído (AWS S3 Lock)
# RISCO: evitar múltiplas escritas simultâneas na mesma tabela

delta.register-table-procedure.enabled=true
# Habilita CALL delta.system.register_table(...)
# Sem isso, o procedimento não existe e o registro de tabelas externas falha

delta.target-max-file-size=128MB
# Tamanho alvo para novos arquivos Parquet escritos pelo Trino
# Afeta apenas escritas via Trino (não via Spark)
```

### Nota: por que s3:// e não s3a://

Com `fs.native-s3.enabled=true`:
- Trino interpreta `s3://bucket/path` usando seu próprio cliente S3
- `s3a://` seria para Hadoop S3A (não disponível no Trino sem hadoop-aws.jar)
- O HMS usa `s3://` no `hive-site.xml` via S3AFileSystem (diferente do Trino)
- Os dois subsistemas têm clientes S3 independentes

---

## 8. Procedimento de Inicialização

### Ordem de startup automática (docker-compose depends_on)

```
1. postgres-metastore    → inicia PostgreSQL (healthcheck: pg_isready)
      ↓ (service_healthy)
2. hive-metastore        → copia hive-site.xml, inicializa schema HMS, sobe Thrift
      ↓ (service_healthy via /dev/tcp)
3. trino-coordinator     → registra-se no discovery, aguarda workers
      ↓ (container started)
4. trino-worker-1        → conecta ao coordinator, registra-se
4. trino-worker-2        → conecta ao coordinator, registra-se
```

### Subir o stack completo (primeira vez)

```bash
# Buildar a imagem customizada do HMS e subir todos os serviços
docker compose up -d

# Acompanhar o progresso do HMS (demora 60-90s)
docker logs -f hive-metastore

# Verificar health de todos os serviços
docker compose ps
```

**Saída esperada do hive-metastore:**
```
=== Aplicando conf de /hive_custom_conf → /opt/hive/conf/ ===
    Copiado: hive-site.xml
=== Hive Metastore entrypoint ===
    DB: jdbc:postgresql://postgres-metastore:5432/metastore
    Inicializando schema HMS no PostgreSQL...
    Schema inicializado com sucesso.
=== Iniciando Hive Metastore Server na porta 9083 ===
```

Em restarts:
```
    Schema já inicializado — pulando initSchema
```

### Registrar as tabelas Delta no Trino

Após todos os containers estarem `healthy`/`running`:

```bash
# Aguardar Trino estar pronto (~30s após subir)
docker exec trino-coordinator trino --execute "SELECT 1"

# Executar o script de inicialização (idempotente)
docker exec trino-coordinator trino -f /etc/trino/init-trino.sql
```

**Saída esperada:**
```
CREATE SCHEMA
DROP TABLE
CALL
DROP TABLE
CALL
... (repete para cada tabela)
"bronze"
"default"
"information_schema"
"ouro"
"prata"
"fat_banco"
"fat_especie"
"fat_uf"
"kpis_nacionais"
"202601","41572553","78521752562.12","1888.79","1621.00","57.32"
```

### Verificar o cluster Trino

```bash
# Verificar nós ativos
docker exec trino-coordinator trino \
  --execute "SELECT node_id, state, version FROM system.runtime.nodes"
```

Saída esperada:
```
"trino-coordinator","active","435"
"3a4fc8e23dac","active","435"
"c5a30bb513d8","active","435"
```

### Web UI do Trino

Acesse: **http://localhost:8086**

- Sem autenticação (modo desenvolvimento)
- Mostra queries em execução, histórico, nós ativos
- Login: qualquer nome de usuário (ex: "admin")

---

## 9. Queries de Referência

### Conectar ao Trino interativamente

```bash
docker exec -it trino-coordinator trino
# Prompt: trino>
```

### Listar catálogos, schemas e tabelas

```sql
-- Catálogos disponíveis
SHOW CATALOGS;
-- delta, system, tpch, tpcds

-- Schemas no catálogo delta
SHOW SCHEMAS FROM delta;
-- bronze, default, information_schema, ouro, prata

-- Tabelas na camada ouro
SHOW TABLES FROM delta.ouro;
-- fat_banco, fat_especie, fat_uf, kpis_nacionais

-- Schema de uma tabela
DESCRIBE delta.ouro.kpis_nacionais;
```

### Queries nas camadas

```sql
-- ── CAMADA OURO (Gold) ────────────────────────────────────────────

-- KPIs nacionais
SELECT
    _ano_mes,
    total_beneficios,
    CAST(vl_total_brasil     AS DECIMAL(18,2)) AS vl_total_brasil,
    CAST(vl_medio_nacional   AS DECIMAL(10,2)) AS vl_medio,
    CAST(vl_mediano_nacional AS DECIMAL(10,2)) AS vl_mediano,
    pct_feminino
FROM delta.ouro.kpis_nacionais;

-- Top 5 UFs por valor total
SELECT uf, total_beneficios, CAST(vl_total AS DECIMAL(18,2)) AS vl_total
FROM delta.ouro.fat_uf
ORDER BY vl_total DESC
LIMIT 5;

-- Distribuição por espécie (top 10)
SELECT especie, total_beneficios
FROM delta.ouro.fat_especie
ORDER BY total_beneficios DESC
LIMIT 10;

-- ── CAMADA PRATA (Silver) ─────────────────────────────────────────

-- Contagem de linhas (41.5M)
SELECT COUNT(*) FROM delta.prata.beneficios_emitidos;

-- Amostra
SELECT * FROM delta.prata.beneficios_emitidos LIMIT 10;

-- Filtro por UF (processado nos workers em paralelo)
SELECT uf, COUNT(*) AS total, SUM(vl_liquido) AS total_liquido
FROM delta.prata.beneficios_emitidos
WHERE uf = 'SP'
GROUP BY uf;

-- ── CAMADA BRONZE (Bronze) ────────────────────────────────────────

-- Dados brutos
SELECT * FROM delta.bronze.beneficios_emitidos LIMIT 5;

-- ── QUERIES DE SISTEMA ────────────────────────────────────────────

-- Queries em execução
SELECT query_id, state, elapsed_time, query
FROM system.runtime.queries
WHERE state = 'RUNNING';

-- Memória por nó
SELECT node_id, total_memory_bytes, free_memory_bytes
FROM system.runtime.nodes;

-- Propriedades do catálogo
SELECT * FROM delta.information_schema.tables;
```

---

## 10. Como Adicionar Novas Tabelas

### Pré-requisito: tabela Delta existe no S3

A tabela deve ter sido escrita via Spark com formato Delta:

```python
# Via PySpark (nos notebooks)
df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("s3a://ouro/pda/minha-tabela")
```

### Registrar no Trino

```sql
-- 1. Garantir que o schema existe (com localização S3)
CREATE SCHEMA IF NOT EXISTS delta.ouro WITH (location = 's3://ouro/');

-- 2. Remover registro anterior (idempotência)
DROP TABLE IF EXISTS delta.ouro.minha_tabela;

-- 3. Registrar a tabela
CALL delta.system.register_table(
    schema_name    => 'ouro',
    table_name     => 'minha_tabela',
    table_location => 's3://ouro/pda/minha-tabela'
);

-- 4. Verificar
SELECT * FROM delta.ouro.minha_tabela LIMIT 5;
```

### Adicionar ao init-trino.sql

Edite `work/init-trino.sql` seguindo o padrão:

```sql
DROP TABLE IF EXISTS delta.ouro.minha_tabela;
CALL delta.system.register_table(
    schema_name    => 'ouro',
    table_name     => 'minha_tabela',
    table_location => 's3://ouro/pda/minha-tabela'
);
```

### Rebuild necessário?

**Não.** O `init-trino.sql` é montado como bind mount. Após editar:

```bash
# Forçar recriação do coordinator para pegar o arquivo atualizado
# (necessário no WSL2 por limitação de cache de bind mount)
docker compose up -d --force-recreate trino-coordinator

# Esperar ~30s e rodar o script
docker exec trino-coordinator trino -f /etc/trino/init-trino.sql
```

---

## 11. Troubleshooting

### HMS não inicia / unhealthy

```bash
# Ver logs detalhados
docker logs hive-metastore --tail 50

# Verificar se PostgreSQL está healthy
docker inspect postgres-metastore --format '{{.State.Health.Status}}'

# Forçar reinício do HMS
docker compose restart hive-metastore
```

**Erro: "Schema já existe" no initSchema**

Já resolvido pelo entrypoint customizado. Se aparecer:
```bash
docker exec postgres-metastore psql -U hive -d metastore -c "SELECT count(*) FROM version;"
# Se retornar 1 linha, o schema existe e o HMS deve detectar automaticamente
```

### Trino worker não aparece no cluster

```bash
# Verificar nós registrados
docker exec trino-coordinator trino \
  --execute "SELECT node_id, state FROM system.runtime.nodes"

# Ver logs do worker
docker logs trino-worker-1 --tail 30

# Reiniciar workers
docker compose restart trino-worker-1 trino-worker-2
```

**Erro: "memory configuration is invalid"**

Verifique a relação entre `-Xmx` no jvm.config e `query.max-memory-per-node`:

```
Regra: max-memory-per-node + (30% × Xmx) < Xmx

Worker:       1024MB + 0.3 × 2048MB = 1638MB < 2048MB ✓
Coordinator:   800MB + 0.3 × 1500MB =  1250MB < 1500MB ✓
```

**Erro: "Unrecognized VM option 'UseBiasedLocking'"**

Trino 435 usa Java 21 que removeu esta flag. Remova do jvm.config.

### Erro ao registrar tabela: "No FileSystem for scheme s3"

**Causa**: o HMS não carregou o `hive-site.xml` com a configuração S3A.

**Verificação:**
```bash
# Confirmar que hive-site.xml foi copiado no boot
docker logs hive-metastore | grep "Copiado"
# Deve mostrar: "    Copiado: hive-site.xml"

# Confirmar conteúdo correto dentro do container
docker exec hive-metastore cat /opt/hive/conf/hive-site.xml
```

**Se não copiado:** verificar permissões e rebuild:
```bash
docker compose build hive-metastore
docker compose up -d hive-metastore
```

### Erro: "Table already exists" no register_table

O script `init-trino.sql` usa `DROP TABLE IF EXISTS` antes de cada `register_table`.
Se rodado antes de uma versão nova do script (container com bind mount antigo):

```bash
# Forçar container com versão atualizada do arquivo
docker compose up -d --force-recreate trino-coordinator
docker exec trino-coordinator trino -f /etc/trino/init-trino.sql
```

### Erro: "Unable to create database path file:/user/hive/warehouse/schema.db"

O HMS tentou criar o schema em filesystem local (sem localização S3).

**Causa**: `CREATE SCHEMA delta.nome;` sem `WITH (location = ...)`.

**Fix**: sempre criar schemas com location explícita:
```sql
CREATE SCHEMA IF NOT EXISTS delta.meu_schema WITH (location = 's3://meu-bucket/');
```

### Erro de memória em queries grandes (41.5M linhas)

```sql
-- Verificar uso de memória durante a query
SELECT node_id, total_memory_bytes, free_memory_bytes
FROM system.runtime.nodes;

-- Adicionar filtros para reduzir volume lido
SELECT COUNT(*) FROM delta.prata.beneficios_emitidos
WHERE uf = 'SP';   -- usa predicate pushdown no Delta Log
```

Se necessário, aumentar memória dos workers no docker-compose.yml:
```yaml
deploy:
  resources:
    limits:
      memory: 4g     # de 2.5g
# E ajustar jvm.config: -Xmx3G
# E config.properties: query.max-memory-per-node=2GB
```

---

## 12. Decisões Técnicas e Armadilhas

### Por que `fs.native-s3.enabled=true` no Trino?

Trino 435 **não inclui** `hadoop-aws.jar` (que contém `S3AFileSystem`). Se usarmos
`s3a://` nas table locations ou `hive.s3.*` nas propriedades, o erro será:

```
ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem
```

A solução: `fs.native-s3.enabled=true` ativa o cliente S3 nativo do Trino (escrito
em Java sem dependência do Hadoop), que funciona com MinIO via `s3.*` properties.

### Por que o HMS precisa do hadoop-aws.jar mas o Trino não?

O **HMS** usa a API de filesystem do Hadoop para verificar e criar caminhos S3 quando
registramos tabelas. O Hive foi escrito em cima do Hadoop e usa `FileSystem.get()`.

O **Trino** tem seu próprio subsistema de filesystem (reescrito), sem dependência do
Hadoop. O conector Delta Lake do Trino usa `TrinoFileSystem` internamente.

### Por que dois clientes S3 diferentes para o mesmo MinIO?

- **HMS** → usa Hadoop S3A (`org.apache.hadoop.fs.s3a.S3AFileSystem`) configurado no `hive-site.xml`
- **Trino** → usa cliente S3 nativo configurado no `delta.properties`

Ambos se conectam ao mesmo MinIO (`minio1:9000`) com as mesmas credenciais, mas via
código completamente diferente. Isso é normal e funciona sem problemas.

### Por que `chown hive:hive /opt/hive/conf/` no Dockerfile?

O diretório `/opt/hive/conf/` é dono do usuário `hive` na imagem base, mas os
**arquivos dentro** dele (como `hive-site.xml` padrão) são do usuário `root`. O
usuário `hive` não pode sobrescrever arquivos de `root`, mesmo sendo dono do
diretório. A solução: `rm -f` deleta o arquivo (o que é permitido para quem tem
`w` no diretório), e `cp` cria um novo arquivo.

### Por que bind mount não atualiza imediatamente no WSL2?

No WSL2 com Docker Desktop, bind mounts de arquivos individuais (não diretórios) às
vezes ficam com cache. Para forçar atualização:

```bash
docker compose up -d --force-recreate trino-coordinator
```

Não é necessário rebuild — só recriar o container.

### Por que `DROP TABLE IF EXISTS` não deleta dados S3?

Tabelas registradas via `CALL delta.system.register_table(...)` são tratadas como
**tabelas externas** pelo Trino. `DROP TABLE` remove apenas o registro no HMS (no
PostgreSQL), **sem** deletar os arquivos Parquet ou o `_delta_log/` no S3.

Isso contrasta com tabelas criadas via `CREATE TABLE ... AS SELECT` (CTAS) no
Trino, que seriam **tabelas gerenciadas** — nesse caso, `DROP TABLE` deletaria
os dados S3.

### Limites de memória — Raciocínio completo

```
Coordinator:
  docker memory limit: não especificado (usa host)
  JVM heap (-Xmx):     1500M
  query.max-memory-per-node: 800M
  Verificação: 800M + 30%×1500M = 800M + 450M = 1250M < 1500M ✓

Worker:
  docker memory limit: 2.5G
  JVM heap (-Xmx):     2G (= 2048M)
  query.max-memory-per-node: 1G (= 1024M)
  Verificação: 1024M + 30%×2048M = 1024M + 614M = 1638M < 2048M ✓
  Overhead fora do heap: 2048M heap + ~400M overhead = ~2.45G < 2.5G ✓
```

A regra 30% é a estimativa do Trino para overhead de heap (buffers internos,
metadata, network buffers). Se `max-memory-per-node + 30%×Xmx >= Xmx`, o Trino
rejeita a configuração na inicialização.
