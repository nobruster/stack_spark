#!/bin/bash
# Wrapper do entrypoint do Hive Metastore
#
# Problemas resolvidos:
#   1. schematool -initSchema falha em restart (schema já existe)
#   2. HIVE_CUSTOM_CONF_DIR bypassed por entrypoint customizado
#   3. S3A filesystem não configurado (inviabiliza s3:// como location)
set -e

# ── Aplica conf customizada (hive-site.xml com S3A) ──────────────
# Nota: HIVE_CUSTOM_CONF_DIR é feature do entrypoint ORIGINAL da imagem.
# Como substituímos o entrypoint, copiamos manualmente para HIVE_CONF_DIR.
if [ -n "$HIVE_CUSTOM_CONF_DIR" ] && [ -d "$HIVE_CUSTOM_CONF_DIR" ]; then
    echo "=== Aplicando conf de $HIVE_CUSTOM_CONF_DIR → /opt/hive/conf/ ==="
    for f in "$HIVE_CUSTOM_CONF_DIR"/*.xml; do
        if [ -f "$f" ]; then
            rm -f "/opt/hive/conf/$(basename "$f")"
            cp "$f" /opt/hive/conf/ && echo "    Copiado: $(basename $f)"
        fi
    done
fi

export HADOOP_CLIENT_OPTS=" -Xmx1G ${SERVICE_OPTS}"

echo "=== Hive Metastore entrypoint ==="
echo "    DB: jdbc:postgresql://postgres-metastore:5432/metastore"

# ── Verifica se schema já existe ─────────────────────────────────
if /opt/hive/bin/schematool -dbType postgres -info 2>/dev/null | grep -q "Hive distribution version"; then
    echo "    Schema já inicializado — pulando initSchema"
else
    echo "    Inicializando schema HMS no PostgreSQL..."
    /opt/hive/bin/schematool -dbType postgres -initSchema
    echo "    Schema inicializado com sucesso."
fi

echo "=== Iniciando Hive Metastore Server na porta 9083 ==="
exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service metastore
