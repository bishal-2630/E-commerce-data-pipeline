FROM astrocrpublic.azurecr.io/runtime:3.4-1


RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-snowflake