#!/bin/bash
# Inicialização do Superset
# Executado uma vez no boot — idempotente (create-admin não falha se já existir)
set -e

echo "=== Superset — inicializando banco de metadados ==="
superset db upgrade

echo "=== Superset — criando admin ==="
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname Superset \
  --email admin@localhost \
  --password admin 2>/dev/null || echo "    Admin já existe — pulando"

echo "=== Superset — inicializando roles e permissões ==="
superset init

echo "=== Superset — iniciando servidor na porta 8088 ==="
exec gunicorn \
  --bind "0.0.0.0:8088" \
  --access-logfile "-" \
  --error-logfile "-" \
  --workers 2 \
  --worker-class gthread \
  --threads 2 \
  --timeout 120 \
  --limit-request-line 0 \
  --limit-request-field_size 0 \
  "superset.app:create_app()"
