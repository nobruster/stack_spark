#!/bin/bash
# Entrypoint para containers Jupyter
# Cria config Spark personalizada com driver.host do container

cp /opt/bitnami/spark/conf/spark-defaults.conf /tmp/spark-defaults.conf
echo "spark.driver.host                ${SPARK_DRIVER_HOST:-jupyter-1}" >> /tmp/spark-defaults.conf
echo "spark.driver.bindAddress         0.0.0.0" >> /tmp/spark-defaults.conf

export SPARK_CONF_DIR=/tmp

exec jupyter notebook \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --NotebookApp.token="${JUPYTER_TOKEN:-spark123}" \
    --NotebookApp.password= \
    --notebook-dir=/opt/bitnami/spark/work
