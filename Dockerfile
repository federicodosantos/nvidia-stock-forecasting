# Gunakan image Airflow yang sama dengan yang Anda tentukan atau default di docker-compose
ARG AIRFLOW_IMAGE_NAME="apache/airflow:3.0.1"
FROM ${AIRFLOW_IMAGE_NAME}

USER root

# Install Java
RUN apt-get update -yqq && \
    apt-get install -yqq --no-install-recommends \
    default-jdk \
    tini && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Ganti kembali ke user airflow
ARG AIRFLOW_UID=50000
USER ${AIRFLOW_UID}

# --- ADD THIS LINE ---
# Install the spark provider
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark