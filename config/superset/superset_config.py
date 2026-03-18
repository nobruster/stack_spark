SECRET_KEY = "stack-prev-local-insecure-key-troque-em-producao"

# Banco de metadados interno do Superset (dashboards, queries salvas, etc.)
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db?check_same_thread=false"

# Desabilita CSRF e Talisman para ambiente local
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False

# Timeout de query mais generoso para queries Trino em tabelas grandes
SUPERSET_WEBSERVER_TIMEOUT = 300
