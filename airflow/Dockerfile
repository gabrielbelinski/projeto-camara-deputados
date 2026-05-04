FROM apache/airflow:3.2.1-python3.14

USER root

RUN apt-get update &&\
    apt-get install -y docker.io &&\
    apt-get clean

USER airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir minio \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-atlassian-jira \
    apache-airflow-providers-databricks \
    apache-airflow-providers-git \
    apache-airflow-providers-github \
    apache-airflow-providers-postgres \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.14.txt"