FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-snowflake==4.3.0" \
    "apache-airflow-providers-amazon==8.2.0" \
    "snowflake-connector-python==3.7.0" \
    "dbt-snowflake==1.7.3" \
    "pyarrow==14.0.1" \
    "pandas==2.1.4" \
    "psycopg2-binary==2.9.9" \
    "boto3==1.34.0"