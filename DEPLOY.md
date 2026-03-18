# Guia de Implantação — Hive Metastore + Trino + PostgreSQL

## Pré-requisitos

```
stack-prev/
├── Dockerfile.hive-metastore          ← imagem customizada do HMS
├── docker-compose.yml
├── config/
│   ├── hive-metastore/
│   │   ├── hive-site.xml              ← config S3A (MinIO)
│   │   └── entrypoint.sh             ← boot idempotente
│   └── trino/
│       ├── catalog/delta.properties   ← catálogo Delta Lake
│       ├── coordinator/               ← config.properties, jvm.config, node.properties, log.properties
│       └── worker/                    ← config.properties, jvm.config, node.properties, log.properties
└── work/
    └── init-trino.sql                 ← registro de tabelas (idempotente)
```

Verifique os arquivos antes de prosseguir:

```bash
ls -la ~/documentos/stack-prev/Dockerfile.hive-metastore
ls -la ~/documentos/stack-prev/config/hive-metastore/
ls -la ~/documentos/stack-prev/config/trino/catalog/
ls -la ~/documentos/stack-prev/work/init-trino.sql
```

---

## Passo 1 — Build da imagem customizada do Hive Metastore

**Por que é necessário um build customizado?**
A imagem oficial `apache/hive:3.1.3` não inclui:
- Driver JDBC do PostgreSQL (necessário para conectar ao backend relacional)
- JARs S3A no classpath correto (necessário para aceitar `s3://` como localização de tabela)
- Entrypoint idempotente (a imagem original falha em restart com "schema already exists")

**O que o `Dockerfile.hive-metastore` faz:**

```dockerfile
FROM apache/hive:3.1.3          # imagem base com Hadoop embutido

USER root

# 1. Driver JDBC PostgreSQL — baixado via ADD (curl/wget não existem na imagem)
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    /opt/hive/lib/postgresql-42.7.3.jar

# 2. JARs S3A — existem em /opt/hadoop/tools/lib/ mas NÃO no classpath do Hive
#    Copiar para /opt/hive/lib/ resolve isso (todos os JARs lá são carregados)
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.1.0.jar \
       /opt/hive/lib/hadoop-aws-3.1.0.jar && \
    cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.271.jar \
       /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar && \
    chmod 644 /opt/hive/lib/hadoop-aws-3.1.0.jar \
              /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar \
              /opt/hive/lib/postgresql-42.7.3.jar

# 3. Entrypoint customizado + permissão no /opt/hive/conf/
COPY config/hive-metastore/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && \
    chown hive:hive /opt/hive/conf/
# chown obrigatório: /opt/hive/conf/ tem arquivos root:root
# O usuário hive não pode sobrescrever arquivos root, mas pode deletar
# do diretório (por ser dono dele) → rm -f + cp resolve no entrypoint

USER hive
ENTRYPOINT ["/entrypoint.sh"]
```

**Executar o build:**

```bash
cd ~/documentos/stack-prev

docker build \
  -t hive-metastore-custom:3.1.3 \
  -f Dockerfile.hive-metastore \
  .
```

Aguarde o download do driver JDBC (~900 KB) e a cópia dos JARs. Esperado:

```
Step 1/8 : FROM apache/hive:3.1.3
Step 2/8 : USER root
Step 3/8 : ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar ...
...
Successfully built <image_id>
Successfully tagged hive-metastore-custom:3.1.3
```

> O `docker compose up -d` faz esse build automaticamente. O passo acima é opcional para testar o build isolado ou pré-buildar antes do deploy.

---

## Passo 2 — Subir PostgreSQL (backend do HMS)

**O que o PostgreSQL armazena:**
Na primeira execução, o `schematool -initSchema` (chamado pelo entrypoint do HMS) cria ~70 tabelas no banco `metastore`. As mais relevantes:

| Tabela | Conteúdo |
|--------|----------|
| `DBS` | Schemas (databases): `ouro`, `prata`, `bronze` |
| `TBLS` | Tabelas registradas |
| `COLUMNS_V2` | Colunas de cada tabela |
| `SDS` | Localização S3 de cada tabela |

```bash
cd ~/documentos/stack-prev

# Subir apenas o PostgreSQL
docker compose up -d postgres-metastore

# Aguardar healthcheck passar (pg_isready)
docker compose ps postgres-metastore
```

**Saída esperada em `Status`:** `healthy`

**Verificar se o banco foi criado:**

```bash
docker exec postgres-metastore psql -U hive -d metastore -c "\l"
```

Deve mostrar o banco `metastore` com owner `hive`.

---

## Passo 3 — Subir o Hive Metastore

**O que o `entrypoint.sh` faz na inicialização:**

```
1. Copia hive-site.xml do volume /hive_custom_conf → /opt/hive/conf/
   (necessário porque substituímos o entrypoint original que fazia isso)

2. Configura JVM: export HADOOP_CLIENT_OPTS="-Xmx1G ${SERVICE_OPTS}"
   SERVICE_OPTS contém as flags JDBC de conexão ao PostgreSQL

3. Verifica se schema já existe (idempotente):
   - se SIM: "Schema já inicializado — pulando initSchema"
   - se NÃO: executa schematool -dbType postgres -initSchema

4. Inicia o servidor Thrift:
   hive --skiphadoopversion --skiphbasecp --service metastore
   (porta 9083)
```

**O que o `hive-site.xml` configura:**
- `fs.s3.impl` e `fs.s3a.impl` → mapeia `s3://` e `s3a://` para `S3AFileSystem`
- `fs.s3a.endpoint` → `http://minio1:9000` (MinIO interno)
- `fs.s3a.path.style.access=true` → obrigatório para MinIO (não AWS virtual-hosted)
- `fs.s3a.connection.ssl.enabled=false` → rede interna Docker sem TLS

```bash
# Subir o HMS (depende do postgres-metastore estar healthy)
docker compose up -d hive-metastore

# Acompanhar inicialização em tempo real (demora 60-90s)
docker logs -f hive-metastore
```

**Saída esperada (primeira execução):**

```
=== Aplicando conf de /hive_custom_conf → /opt/hive/conf/ ===
    Copiado: hive-site.xml
=== Hive Metastore entrypoint ===
    DB: jdbc:postgresql://postgres-metastore:5432/metastore
    Inicializando schema HMS no PostgreSQL...
    Schema inicializado com sucesso.
=== Iniciando Hive Metastore Server na porta 9083 ===
...
Starting Hive Metastore Server
```

**Saída esperada (restarts subsequentes):**

```
=== Aplicando conf de /hive_custom_conf → /opt/hive/conf/ ===
    Copiado: hive-site.xml
=== Hive Metastore entrypoint ===
    DB: jdbc:postgresql://postgres-metastore:5432/metastore
    Schema já inicializado — pulando initSchema
=== Iniciando Hive Metastore Server na porta 9083 ===
```

**Verificar healthcheck:**

```bash
docker compose ps hive-metastore
# Status deve mostrar: healthy
# (pode demorar até 200s: 20 retries × 10s, com 90s de start_period)
```

---

## Passo 4 — Subir o Trino (Coordinator + Workers)

**Como o Trino se conecta ao HMS:**
O arquivo `config/trino/catalog/delta.properties` define:
- `hive.metastore.uri=thrift://hive-metastore:9083` → Trino consulta o HMS via protocolo Thrift
- `fs.native-s3.enabled=true` → usa cliente S3 nativo do Trino (sem Hadoop S3A, que não existe no Trino 435)
- `delta.register-table-procedure.enabled=true` → habilita `CALL delta.system.register_table(...)`

> **Importante:** O Trino 435 **não inclui** `hadoop-aws.jar`. Usar `s3a://` em qualquer config do Trino causaria `ClassNotFoundException: S3AFileSystem`. O HMS usa S3A; o Trino usa S3 nativo — são clientes independentes para o mesmo MinIO.

```bash
# Subir coordinator e workers
docker compose up -d trino-coordinator trino-worker-1 trino-worker-2

# Verificar status de todos os containers da stack
docker compose ps
```

**Status esperado de todos os serviços:**

```
NAME                    STATUS
postgres-metastore      Up (healthy)
hive-metastore          Up (healthy)
trino-coordinator       Up
trino-worker-1          Up
trino-worker-2          Up
```

**Verificar nós Trino registrados (~30s após subir):**

```bash
docker exec trino-coordinator trino \
  --execute "SELECT node_id, state, version FROM system.runtime.nodes"
```

**Saída esperada:**

```
"trino-coordinator","active","435"
"3a4fc8e23dac","active","435"
"c5a30bb513d8","active","435"
```

Se os workers não aparecerem após 60s, ver troubleshooting abaixo.

---

## Passo 5 — Subir o stack completo (atalho)

Os passos 2, 3 e 4 podem ser feitos em um único comando. O `docker-compose.yml` usa `depends_on` com `condition: service_healthy` para garantir a ordem:

```
postgres-metastore (healthy) → hive-metastore (healthy) → trino-coordinator → workers
```

```bash
cd ~/documentos/stack-prev

# Build + deploy de todos os serviços
docker compose up -d

# Monitorar inicialização do HMS (mais lento)
docker logs -f hive-metastore
# Ctrl+C quando aparecer "Starting Hive Metastore Server"

# Verificar todos os containers
docker compose ps
```

---

## Passo 6 — Registrar as tabelas Delta no Trino

O script `work/init-trino.sql` é **idempotente**: executa `DROP TABLE IF EXISTS` antes de cada `register_table`. `DROP TABLE` em tabelas externas **não apaga os dados no S3**.

```bash
# Aguardar Trino estar pronto (~30s)
docker exec trino-coordinator trino --execute "SELECT 1"

# Executar o script de inicialização
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

---

## Passo 7 — Verificação final

```bash
# Conectar ao Trino interativamente
docker exec -it trino-coordinator trino

# Listar catálogos
trino> SHOW CATALOGS;

# Listar schemas do catálogo delta
trino> SHOW SCHEMAS FROM delta;

# Listar tabelas na camada ouro
trino> SHOW TABLES FROM delta.ouro;

# Consulta de validação
trino> SELECT * FROM delta.ouro.kpis_nacionais;

# Sair
trino> quit
```

**Web UI do Trino:** http://localhost:8086 (login: qualquer nome, ex: "admin")

**Verificar metadados no PostgreSQL:**

```bash
docker exec postgres-metastore psql -U hive -d metastore -c "
SELECT t.TBL_NAME, d.NAME AS schema, s.LOCATION
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
JOIN SDS s ON t.SD_ID = s.SD_ID;"
```

---

## Troubleshooting Rápido

**HMS unhealthy / não inicia:**
```bash
docker logs hive-metastore --tail 50
docker compose restart hive-metastore
```

**Erro "No FileSystem for scheme s3":**
```bash
# Confirmar que hive-site.xml foi copiado
docker logs hive-metastore | grep "Copiado"
# Se não aparecer:
docker compose build hive-metastore && docker compose up -d hive-metastore
```

**Workers não aparecem no cluster:**
```bash
docker logs trino-worker-1 --tail 30
docker compose restart trino-worker-1 trino-worker-2
```

**Erro "memory configuration is invalid" (Trino):**
```
Regra: query.max-memory-per-node + (30% × Xmx) < Xmx
Worker:      1024MB + 0.3×2048MB = 1638MB < 2048MB ✓
Coordinator:  800MB + 0.3×1500MB = 1250MB < 1500MB ✓
```

**Erro "Unrecognized VM option 'UseBiasedLocking'":**
Remover essa flag do `jvm.config` — foi removida no Java 15, Trino 435 usa Java 21.

**Bind mount não atualiza (WSL2):**
```bash
docker compose up -d --force-recreate trino-coordinator
```

---

## Resumo de Portas

| Serviço | Porta | Acesso |
|---------|-------|--------|
| Trino Web UI | http://localhost:8086 | Browser |
| Trino Thrift/JDBC | 8080 (interno) | Drivers JDBC |
| Hive Metastore | 9083 (interno) | Somente Trino |
| PostgreSQL | 5432 (interno) | Somente HMS |
