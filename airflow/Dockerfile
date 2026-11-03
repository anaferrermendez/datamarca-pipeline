FROM apache/airflow:3.0.0-python3.12

USER root

# Instalar Java (necesario para PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Instalar PySpark y las dependencias de tu pipeline
RUN pip install --no-cache-dir pyspark==4.2.1