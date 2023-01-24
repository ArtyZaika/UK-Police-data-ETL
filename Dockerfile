# First-time build can take upto 10 mins.
FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER root
RUN apt-get update -qq \
    && apt-get install wget -qqq \   
    && apt-get install -y --no-install-recommends \
            openjdk-11-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY scripts scripts
RUN chown -R airflow: ${AIRFLOW_HOME}
RUN chmod +x scripts

EXPOSE 8080

USER $AIRFLOW_UID
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
