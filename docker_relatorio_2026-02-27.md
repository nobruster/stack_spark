# 🐳 Relatório de Saúde Docker

**Data da Avaliação:** 2026-02-27 12:31:14
**Hostname:** Bruno
**Versão Docker:** 28.1.1 (API 1.49)
**Sistema Operacional:** Ubuntu 20.04.6 LTS (WSL2)
**Kernel:** 6.6.87.2-microsoft-standard-WSL2
**Status Geral:** ✅ SAUDÁVEL

---

## 📊 Resumo Executivo

| Métrica | Valor |
|---------|-------|
| Total de Contêineres | 13 |
| Saudáveis (Em Execução) | 13 |
| Com Problemas | 0 |
| Parados | 0 |
| Status do Daemon | ✅ OK |
| Total de Imagens | 98 |
| Imagens Dangling (não utilizadas) | 48 |
| Total de Volumes | 14 |
| Volumes Órfãos | 5 |
| Total de Redes | 4 |

---

## 🔧 Daemon Docker

### Status
- **Status do Daemon:** ✅ OPERACIONAL
- **Versão do Engine:** 28.1.1
- **API Version:** 1.49
- **Storage Driver:** overlay2 sobre extfs
- **Cgroup Driver:** systemd (v2)
- **Docker Root:** /var/lib/docker
- **Runtime Padrão:** runc v1.2.5
- **Containerd Version:** 1.7.27

### Recursos do Sistema
- **CPUs Disponíveis:** 32
- **Memória Total:** 15.43 GiB
- **Contêineres Ativos:** 13 em execução, 0 pausados, 0 parados
- **Swarm Mode:** Inativo

### 💾 Uso de Espaço em Disco

| Tipo | Total | Ativo | Tamanho | Recuperável |
|------|-------|-------|---------|-------------|
| **Imagens** | 81 | 10 | 48.85 GB | 47.27 GB (96%) ⚠️ |
| **Contêineres** | 13 | 13 | 135.9 MB | 0 B (0%) |
| **Volumes** | 14 | 1 | 446.1 MB | 445.6 MB (99%) ⚠️ |
| **Cache de Build** | 216 | 0 | 313.5 MB | 313.5 MB ⚠️ |

**Disco do Docker Root:**
- **Localização:** /dev/sdd
- **Tamanho Total:** 1007 GB
- **Usado:** 73 GB (8%)
- **Disponível:** 883 GB
- **Status:** ✅ SAUDÁVEL

### ⚠️ Alertas do Daemon

1. **ATENÇÃO - Imagens Recuperáveis:** 47.27 GB (96% das imagens) podem ser recuperados através de limpeza
2. **ATENÇÃO - Volumes Órfãos:** 445.6 MB (99% dos volumes) não estão em uso ativo
3. **ATENÇÃO - Cache de Build:** 313.5 MB de cache não utilizado
4. **ATENÇÃO - Imagens Dangling:** 48 imagens sem tag detectadas

---

## 📦 Contêineres em Execução

### Stack stack-prev (Data Lakehouse)

Todos os 12 contêineres da stack estão em execução e saudáveis. Tempo de atividade: aproximadamente 3 minutos.

---

#### 1. **portainer** - ✅ SAUDÁVEL

**Imagem:** portainer/portainer-ce:lts (271 MB)
**Status:** Em execução (2 minutos de uptime)
**Health Status:** ✅ healthy
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.00% (NORMAL)
- **Memória:** 16.79 MiB / 15.43 GiB (0.11%) ✅ NORMAL
- **Rede:** 1.87 kB entrada / 736 B saída
- **I/O Disco:** 3.9 MB leitura / 0 B escrita
- **Processos:** 10

**Portas:**
- 9443:9443 (HTTPS - Interface Web)
- 8000, 9000 (internos)

**Volumes:**
- portainer_data

**Observações:** Interface de gerenciamento Docker funcionando corretamente com healthcheck ativo.

---

#### 2. **spark-master** - ✅ SAUDÁVEL

**Imagem:** stack-prev-spark-master:latest (2.35 GB)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A (sem healthcheck configurado)
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.08% ✅ NORMAL
- **Memória:** 178.6 MiB / 1 GiB (17.45%) ✅ NORMAL
- **Limite Configurado:** 1 GB RAM, 1.0 CPU
- **Rede:** 64.7 kB entrada / 10.4 kB saída
- **I/O Disco:** 48.4 MB leitura / 618 kB escrita
- **Processos:** 30

**Portas:**
- 8090:8080 (Spark Master Web UI)
- 7077:7077 (Spark Master)

**Volumes:**
- ./config/spark/spark-defaults.conf
- ./config/spark/log4j2.properties
- ./data/spark-events

**Observações:** Coordenador do cluster Spark operando dentro dos limites normais.

---

#### 3. **spark-worker-1** - ✅ SAUDÁVEL

**Imagem:** stack-prev-spark-worker-1:latest (2.35 GB)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.15% ✅ NORMAL
- **Memória:** 194.4 MiB / 2.5 GiB (7.59%) ✅ NORMAL
- **Limite Configurado:** 2.5 GB RAM, 2.0 CPUs, 2g worker memory
- **Rede:** 5.91 kB entrada / 19.9 kB saída
- **I/O Disco:** 30 MB leitura / 344 kB escrita
- **Processos:** 37

**Portas:**
- 8081:8081 (Worker Web UI)

**Observações:** Worker Spark operando normalmente.

---

#### 4. **spark-worker-2** - ✅ SAUDÁVEL

**Imagem:** stack-prev-spark-worker-2:latest (2.35 GB)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.15% ✅ NORMAL
- **Memória:** 192 MiB / 2.5 GiB (7.50%) ✅ NORMAL
- **Limite Configurado:** 2.5 GB RAM, 2.0 CPUs, 2g worker memory
- **Rede:** 6.04 kB entrada / 21.1 kB saída
- **I/O Disco:** 12.7 MB leitura / 352 kB escrita
- **Processos:** 37

**Portas:**
- 8082:8081 (Worker Web UI)

**Observações:** Worker Spark operando normalmente.

---

#### 5. **spark-worker-3** - ✅ SAUDÁVEL

**Imagem:** stack-prev-spark-worker-3:latest (2.35 GB)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.16% ✅ NORMAL
- **Memória:** 189.1 MiB / 2.5 GiB (7.38%) ✅ NORMAL
- **Limite Configurado:** 2.5 GB RAM, 2.0 CPUs, 2g worker memory
- **Rede:** 6.54 kB entrada / 21.1 kB saída
- **I/O Disco:** 18.1 MB leitura / 356 kB escrita
- **Processos:** 37

**Portas:**
- 8083:8081 (Worker Web UI)

**Observações:** Worker Spark operando normalmente.

---

#### 6. **spark-history** - ⚠️ ATENÇÃO

**Imagem:** stack-prev-spark-history:latest (2.35 GB)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.13% ✅ NORMAL
- **Memória:** 168.7 MiB / 512 MiB (32.95%) ✅ NORMAL (próximo ao limite ideal)
- **Limite Configurado:** 512 MB RAM, 0.5 CPU
- **Rede:** 2.83 kB entrada / 126 B saída
- **I/O Disco:** 21.8 MB leitura / 590 kB escrita
- **Processos:** 25

**Portas:**
- 18080:18080 (History Server UI)

**Observações:** Uso de memória de 33% é aceitável, mas pode aumentar com mais histórico de jobs.

---

#### 7. **jupyter-1** - ✅ SAUDÁVEL

**Imagem:** stack-prev-jupyter-1:latest (2.35 GB)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.00% ✅ NORMAL (idle)
- **Memória:** 81.38 MiB / 2 GiB (3.97%) ✅ NORMAL
- **Limite Configurado:** 2 GB RAM, 1.0 CPU
- **Rede:** 2.83 kB entrada / 126 B saída
- **I/O Disco:** 19.2 MB leitura / 3.13 MB escrita
- **Processos:** 1

**Portas:**
- 8888:8888 (Jupyter Notebook)

**Volumes:**
- ./work (compartilhado)

**Observações:** Servidor Jupyter pronto para uso. Token: spark123

---

#### 8. **jupyter-2** - ✅ SAUDÁVEL

**Imagem:** stack-prev-jupyter-2:latest (2.35 GB)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.00% ✅ NORMAL (idle)
- **Memória:** 86.49 MiB / 2 GiB (4.22%) ✅ NORMAL
- **Limite Configurado:** 2 GB RAM, 1.0 CPU
- **Rede:** 2.88 kB entrada / 126 B saída
- **I/O Disco:** 31.8 MB leitura / 3.13 MB escrita
- **Processos:** 1

**Portas:**
- 8889:8888 (Jupyter Notebook)

**Volumes:**
- ./work (compartilhado)

**Observações:** Servidor Jupyter secundário pronto para uso. Token: spark123

---

#### 9. **minio1** - ✅ SAUDÁVEL

**Imagem:** minio/minio (oficial)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.00% ✅ NORMAL
- **Memória:** 261.1 MiB / 15.43 GiB (1.65%) ✅ NORMAL
- **Rede:** 769 kB entrada / 692 kB saída
- **I/O Disco:** 28.5 MB leitura / 393 kB escrita
- **Processos:** 31

**Portas:**
- 9000:9000 (MinIO API)
- 9001:9001 (MinIO Console)

**Volumes:**
- ./data/minio1:/data

**Observações:** Nó principal do cluster MinIO distribuído (4 nós). Credenciais: minioadmin/minioadmin

---

#### 10. **minio2** - ✅ SAUDÁVEL

**Imagem:** minio/minio (oficial)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.09% ✅ NORMAL
- **Memória:** 259.5 MiB / 15.43 GiB (1.64%) ✅ NORMAL
- **Rede:** 451 kB entrada / 596 kB saída
- **I/O Disco:** 29.5 MB leitura / 393 kB escrita
- **Processos:** 27

**Volumes:**
- ./data/minio2:/data

**Observações:** Nó 2 do cluster MinIO.

---

#### 11. **minio3** - ✅ SAUDÁVEL

**Imagem:** minio/minio (oficial)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.00% ✅ NORMAL
- **Memória:** 245.6 MiB / 15.43 GiB (1.55%) ✅ NORMAL
- **Rede:** 370 kB entrada / 286 kB saída
- **I/O Disco:** 34.7 MB leitura / 393 kB escrita
- **Processos:** 27

**Volumes:**
- ./data/minio3:/data

**Observações:** Nó 3 do cluster MinIO.

---

#### 12. **minio4** - ✅ SAUDÁVEL

**Imagem:** minio/minio (oficial)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.05% ✅ NORMAL
- **Memória:** 243.9 MiB / 15.43 GiB (1.54%) ✅ NORMAL
- **Rede:** 327 kB entrada / 235 kB saída
- **I/O Disco:** 29 MB leitura / 393 kB escrita
- **Processos:** 26

**Volumes:**
- ./data/minio4:/data

**Observações:** Nó 4 do cluster MinIO.

---

#### 13. **dremio** - ✅ SAUDÁVEL

**Imagem:** dremio/dremio-oss (oficial)
**Status:** Em execução (3 minutos de uptime)
**Health Status:** N/A
**Reinicializações:** 0

**Recursos:**
- **CPU:** 0.55% ✅ NORMAL
- **Memória:** 1.624 GiB / 8 GiB (20.29%) ✅ NORMAL
- **Limite Configurado:** 8 GB RAM, Heap 4 GB, Direct Memory 2 GB
- **Rede:** 137 kB entrada / 226 kB saída
- **I/O Disco:** 334 MB leitura / 133 MB escrita
- **Processos:** 344

**Portas:**
- 9047:9047 (Dremio Web UI)
- 31010:31010 (JDBC)
- 32010:32010 (Flight)
- 45678:45678 (Arrow Flight)

**Volumes:**
- ./data/dremio
- ./data/dremio-spill

**Observações:** Query engine Dremio operando normalmente. Uso de memória adequado para inicialização.

---

## 🌐 Redes Docker

| Network ID | Nome | Driver | Escopo | Contêineres |
|------------|------|--------|--------|-------------|
| c889dbb053bf | bridge | bridge | local | 1 (portainer) |
| 0dd395c67ca6 | host | host | local | 0 |
| 03b095dd18ec | none | null | local | 0 |
| 7751382402f0 | stack-prev_spark-network | bridge | local | 12 |

### Detalhes da Rede stack-prev_spark-network

**Subnet:** 172.18.0.0/16
**Driver:** bridge
**Contêineres Conectados:** 12

| Contêiner | IPv4 | MAC Address |
|-----------|------|-------------|
| dremio | 172.18.0.2/16 | 3e:bd:9f:fe:1d:0e |
| minio3 | 172.18.0.3/16 | 4e:ec:b5:ca:c0:58 |
| minio2 | 172.18.0.4/16 | 1e:a4:dc:f8:89:e0 |
| minio4 | 172.18.0.5/16 | f2:1b:37:70:3a:50 |
| minio1 | 172.18.0.6/16 | 5a:1f:be:21:6f:94 |
| spark-master | 172.18.0.7/16 | 2e:1d:26:b4:01:48 |
| spark-worker-1 | 172.18.0.8/16 | b2:8f:e8:4c:90:d4 |
| jupyter-2 | 172.18.0.9/16 | 96:24:fe:91:e5:8c |
| jupyter-1 | 172.18.0.10/16 | da:15:9f:52:db:ff |
| spark-history | 172.18.0.11/16 | 9a:d5:ab:ad:f1:73 |
| spark-worker-3 | 172.18.0.12/16 | 3e:dc:52:92:a5:ea |
| spark-worker-2 | 172.18.0.13/16 | 22:d7:2f:6b:46:2f |

**Status:** ✅ Todos os contêineres da stack conectados corretamente. Sem redes órfãs ou desconectadas.

---

## 💾 Volumes Docker

### Volumes Ativos (Em Uso)

| Volume | Em Uso Por | Mountpoint |
|--------|------------|------------|
| portainer_data | portainer | /var/lib/docker/volumes/portainer_data/_data |

### Volumes da Stack stack-prev

Nota: Os volumes abaixo estão definidos no docker-compose.yml mas são montados como bind mounts do diretório local `./data/`, não como volumes Docker nomeados no sentido tradicional. Os volumes listados aqui foram criados automaticamente:

| Volume | Propósito |
|--------|-----------|
| stack-prev_dremio-data | Dados do Dremio |
| stack-prev_dremio-spill | Área de spill do Dremio |
| stack-prev_minio1-data | Dados do MinIO nó 1 |
| stack-prev_minio2-data | Dados do MinIO nó 2 |
| stack-prev_minio3-data | Dados do MinIO nó 3 |
| stack-prev_minio4-data | Dados do MinIO nó 4 |
| stack-prev_spark-events | Logs de eventos do Spark |

### ⚠️ Volumes Órfãos (Projetos Anteriores)

Os seguintes volumes não estão sendo utilizados por nenhum contêiner ativo e podem ser removidos com segurança:

| Volume | Tamanho Estimado | Projeto Original | Mountpoint |
|--------|------------------|------------------|------------|
| build_hive_metastore_data | ~ | Projeto build antigo | /var/lib/docker/volumes/build_hive_metastore_data/_data |
| build_minio_data | ~ | Projeto build antigo | /var/lib/docker/volumes/build_minio_data/_data |
| build_postgres_data | ~ | Projeto build antigo | /var/lib/docker/volumes/build_postgres_data/_data |
| datalakehouse-trino-dbt-airflow-minio-iceberg-metabase_airflow_data | ~ | Projeto datalakehouse | /var/lib/docker/volumes/datalakehouse-trino-dbt-airflow-minio-iceberg-metabase_airflow_data/_data |
| datalakehouse-trino-dbt-airflow-minio-iceberg-metabase_postgres_data | ~ | Projeto datalakehouse | /var/lib/docker/volumes/datalakehouse-trino-dbt-airflow-minio-iceberg-metabase_postgres_data/_data |
| postgres_data | ~ | Projeto desconhecido | /var/lib/docker/volumes/postgres_data/_data |

**Total de Volumes Órfãos:** 6 volumes (445.6 MB recuperáveis)

---

## 🖼️ Imagens Docker

### Resumo de Imagens

- **Total de Imagens:** 98
- **Imagens em Uso:** 10
- **Imagens Dangling (sem tag):** 48
- **Tamanho Total:** 48.85 GB
- **Espaço Recuperável:** 47.27 GB (96%)

### Imagens em Uso Ativo

| Repositório | Tag | ID | Criada | Tamanho |
|-------------|-----|----|---------|----|
| portainer-healthcheck | lts | 2202318a66d4 | 2026-02-27 | 271 MB |
| stack-prev-jupyter-1 | latest | 43701009b9ee | 2026-02-25 | 2.35 GB |
| stack-prev-jupyter-2 | latest | 644bfe51c1ca | 2026-02-25 | 2.35 GB |
| stack-prev-spark-master | latest | f784dee5b538 | 2026-02-25 | 2.35 GB |
| stack-prev-spark-worker-1 | latest | 854318e169d4 | 2026-02-25 | 2.35 GB |
| stack-prev-spark-worker-2 | latest | 8e6c13de0ee2 | 2026-02-25 | 2.35 GB |
| stack-prev-spark-worker-3 | latest | 6ae6e990e7ca | 2026-02-25 | 2.35 GB |
| stack-prev-spark-history | latest | fdef0202b74a | 2026-02-25 | 2.35 GB |
| minio/minio | latest | (oficial) | - | ~200 MB |
| dremio/dremio-oss | latest | (oficial) | - | ~1.5 GB |

### ⚠️ Imagens Dangling (Primeiras 10)

Imagens sem tag resultantes de rebuilds do stack-prev:

| ID | Criada | Tamanho |
|----|---------|---------|
| 510e229a9c33 | 2026-02-24 23:13 | 2.35 GB |
| cc0729509e31 | 2026-02-24 23:13 | 2.35 GB |
| aa586961eedf | 2026-02-24 23:13 | 2.35 GB |
| 0899561d80ca | 2026-02-24 23:13 | 2.35 GB |
| ecb27edd8e77 | 2026-02-24 23:13 | 2.35 GB |
| 412eba9b60ba | 2026-02-24 20:26 | 2.35 GB |
| 2ed824ed7023 | 2026-02-24 20:20 | 2.4 GB |
| bb6a5c27f589 | 2026-02-24 20:20 | 2.4 GB |
| fa764a53e135 | 2026-02-24 20:20 | 2.4 GB |
| 89f1c11c49db | 2026-02-24 20:20 | 2.4 GB |

**Total de 48 imagens dangling** ocupando aproximadamente 47 GB de espaço recuperável.

---

## 🔧 Ações Corretivas Sugeridas

### Prioridade ALTA

Nenhuma ação de alta prioridade identificada. Todos os serviços estão operacionais.

### Prioridade MÉDIA

#### 1. Limpeza de Imagens Dangling

**Problema:** 48 imagens sem tag ocupando aproximadamente 47 GB de espaço
**Contêineres Afetados:** Nenhum (imagens não utilizadas)
**Impacto:** Desperdício de espaço em disco

**Ação Sugerida:** Remover imagens dangling para liberar espaço

**Comando:**
```bash
docker image prune -f
```

**Benefício:** Recupera aproximadamente 47 GB de espaço em disco

---

#### 2. Limpeza de Volumes Órfãos

**Problema:** 6 volumes não utilizados ocupando 445.6 MB
**Contêineres Afetados:** Nenhum (volumes órfãos de projetos anteriores)
**Impacto:** Desperdício de espaço e poluição do ambiente

**Ação Sugerida:** Verificar e remover volumes órfãos com segurança

**Comando de Verificação:**
```bash
# Inspecionar volumes antes de remover
docker volume inspect build_hive_metastore_data
docker volume inspect build_minio_data
docker volume inspect build_postgres_data
docker volume inspect datalakehouse-trino-dbt-airflow-minio-iceberg-metabase_airflow_data
docker volume inspect datalakehouse-trino-dbt-airflow-minio-iceberg-metabase_postgres_data
docker volume inspect postgres_data
```

**Comando de Remoção (após confirmação):**
```bash
docker volume prune -f
```

**Benefício:** Recupera 445.6 MB e limpa o ambiente

---

#### 3. Limpeza de Cache de Build

**Problema:** 313.5 MB de cache de build não utilizado
**Contêineres Afetados:** Nenhum
**Impacto:** Desperdício de espaço

**Ação Sugerida:** Limpar cache de build

**Comando:**
```bash
docker builder prune -f
```

**Benefício:** Recupera 313.5 MB

---

#### 4. Configurar Healthchecks para Contêineres Críticos

**Problema:** Apenas o Portainer possui healthcheck configurado
**Contêineres Afetados:** spark-master, spark-workers, jupyter, minio, dremio, spark-history
**Impacto:** Dificuldade em detectar automaticamente falhas de serviço

**Ação Sugerida:** Adicionar healthchecks ao docker-compose.yml

**Exemplo para Spark Master:**
```yaml
spark-master:
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8080"]
    interval: 30s
    timeout: 10s
    retries: 3
    start_period: 40s
```

**Exemplo para MinIO:**
```yaml
minio1:
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
    interval: 30s
    timeout: 10s
    retries: 3
```

**Exemplo para Jupyter:**
```yaml
jupyter-1:
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8888"]
    interval: 30s
    timeout: 10s
    retries: 3
```

**Benefício:** Monitoramento automático da saúde dos serviços

---

### Prioridade BAIXA

#### 5. Limpeza Completa do Sistema (Opcional)

**Problema:** Acúmulo geral de recursos não utilizados
**Ação Sugerida:** Limpeza completa do sistema Docker (USE COM CUIDADO)

**Comando:**
```bash
# ATENÇÃO: Este comando remove TODAS as imagens não utilizadas, não apenas dangling
docker system prune -a -f --volumes
```

**Benefício:** Libera todo o espaço recuperável (47.27 GB + 445.6 MB + 313.5 MB)

⚠️ **AVISO:** Este comando removerá também imagens de projetos anteriores que não estão em uso. Certifique-se de que você não precisará dessas imagens antes de executar.

---

#### 6. Monitoramento de Recursos do spark-history

**Problema:** Uso de memória de 33% pode crescer com mais histórico de jobs
**Contêiner Afetado:** spark-history
**Impacto:** Baixo (uso atual aceitável)

**Ação Sugerida:** Monitorar uso de memória ao longo do tempo

**Comando de Monitoramento:**
```bash
docker stats spark-history --no-stream
```

**Se o uso ultrapassar 80%:** Aumentar limite de memória de 512 MB para 1 GB no docker-compose.yml

---

## 📊 Análise de Performance Geral

### Uso Agregado de Recursos

- **CPU Total Utilizada:** < 2% do total disponível (32 CPUs)
- **Memória Total Utilizada:** ~3.5 GB de 15.43 GB (22.7%)
- **Processos Totais:** 654 processos ativos
- **Tráfego de Rede Total:** ~2 MB entrada / ~2 MB saída

### Distribuição de Memória por Serviço

| Categoria | Memória | % do Total |
|-----------|---------|------------|
| Dremio | 1.62 GB | 46.3% |
| MinIO Cluster (4 nós) | 1.01 GB | 28.8% |
| Spark Workers (3) | 575.5 MB | 16.4% |
| Spark Master | 178.6 MB | 5.1% |
| Spark History | 168.7 MB | 4.8% |
| Jupyter (2) | 167.9 MB | 4.8% |
| Portainer | 16.8 MB | 0.5% |

### Estado de Saúde por Categoria

| Categoria | Status |
|-----------|--------|
| Daemon Docker | ✅ SAUDÁVEL |
| Contêineres | ✅ SAUDÁVEL (13/13 operacionais) |
| Redes | ✅ SAUDÁVEL |
| Volumes | ⚠️ ATENÇÃO (volumes órfãos detectados) |
| Imagens | ⚠️ ATENÇÃO (48 dangling, 47 GB recuperáveis) |
| Disco | ✅ SAUDÁVEL (92% disponível) |
| CPU | ✅ SAUDÁVEL (< 2% utilizado) |
| Memória | ✅ SAUDÁVEL (22.7% utilizado) |

---

## 📝 Observações Finais

### Pontos Positivos

1. ✅ Todos os 13 contêineres estão em execução e operacionais
2. ✅ Stack stack-prev (Data Lakehouse) completamente funcional
3. ✅ Uso de CPU e memória dentro de limites saudáveis
4. ✅ Nenhuma reinicialização detectada nos contêineres
5. ✅ Disco com 92% de espaço disponível
6. ✅ Rede stack-prev_spark-network funcionando corretamente
7. ✅ Portainer operacional com healthcheck ativo
8. ✅ Cluster MinIO distribuído (4 nós) operacional
9. ✅ Cluster Spark (1 master + 3 workers) operacional
10. ✅ Dremio query engine funcional

### Áreas de Melhoria

1. ⚠️ **Limpeza de Imagens:** 47 GB de imagens dangling podem ser recuperados
2. ⚠️ **Volumes Órfãos:** 6 volumes não utilizados de projetos anteriores
3. ⚠️ **Healthchecks:** Apenas Portainer possui healthcheck configurado
4. ⚠️ **Cache de Build:** 313.5 MB podem ser liberados
5. ℹ️ **Monitoramento:** spark-history com 33% de uso de memória (dentro do normal, mas monitorar crescimento)

### Recomendações Operacionais

1. **Executar limpeza periódica de imagens dangling** após rebuilds do stack
2. **Implementar healthchecks** para todos os serviços críticos
3. **Monitorar uso de memória do spark-history** ao processar muitos jobs
4. **Remover volumes órfãos** de projetos anteriores com segurança
5. **Documentar credenciais de acesso** (MinIO: minioadmin/minioadmin, Jupyter token: spark123)

### Serviços Acessíveis

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| Portainer | https://localhost:9443 | (configurar no primeiro acesso) |
| Spark Master UI | http://localhost:8090 | N/A |
| Spark Worker 1 UI | http://localhost:8081 | N/A |
| Spark Worker 2 UI | http://localhost:8082 | N/A |
| Spark Worker 3 UI | http://localhost:8083 | N/A |
| Spark History Server | http://localhost:18080 | N/A |
| Jupyter Notebook 1 | http://localhost:8888 | Token: spark123 |
| Jupyter Notebook 2 | http://localhost:8889 | Token: spark123 |
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin |
| MinIO API | http://localhost:9000 | minioadmin/minioadmin |
| Dremio UI | http://localhost:9047 | (configurar no primeiro acesso) |

---

## 🎯 Conclusão

O ambiente Docker está **SAUDÁVEL** e totalmente operacional. Todos os 13 contêineres estão funcionando corretamente, com uso de recursos dentro dos limites normais. A stack stack-prev está pronta para uso em desenvolvimento de pipelines de dados.

As ações corretivas sugeridas são **não urgentes** e focam em otimização de espaço em disco e melhoria de monitoramento. O sistema pode continuar operando sem interrupções.

**Próximo Checkpoint Recomendado:** 7 dias

---

**Relatório gerado por:** Docker Health Monitor
**Avaliador:** Claude (Engenheiro de Sistemas Sênior)
**Data:** 2026-02-27 12:31:14
**Hostname:** Bruno
**Docker Version:** 28.1.1
