# Airflow for Pi5 (ADR-0010): the official image plus the ClickHouse client for the check DAGs.
# FAB and Postgres providers are already in the image. Built on Pi5: docker compose build.
FROM apache/airflow:3.1.0

# The constraints of this Airflow release pin every shared dependency (lz4, certifi, urllib3...);
# apache-airflow is listed so pip can never upgrade it while resolving.
ARG AIRFLOW_VERSION=3.1.0
ARG PYTHON_VERSION=3.12
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        "clickhouse-connect==1.9.0" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
