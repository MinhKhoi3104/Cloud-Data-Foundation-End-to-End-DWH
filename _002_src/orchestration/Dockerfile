FROM apache/airflow:2.10.3-python3.8

# =====================================================
# ROOT: install system deps
# =====================================================
USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    procps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# =====================================================
# Spark client (ONLY client, no worker)
# =====================================================
ENV SPARK_VERSION=3.5.3

RUN curl -fL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt/ \
 && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# =====================================================
# Spark JARs (Redshift, S3, Iceberg...)
# =====================================================
RUN mkdir -p /opt/spark/jars
COPY jars/ /opt/spark/jars/

# =====================================================
# Airflow project structure
# =====================================================
RUN mkdir -p /opt/airflow/_002_src \
 && chown -R airflow: /opt/spark /opt/airflow

# =====================================================
# Airflow user
# =====================================================
USER airflow

# Python deps cho ETL
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
