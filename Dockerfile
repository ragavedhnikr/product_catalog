FROM apache/airflow:3.4.0-python3.12

USER airflow

# Keep dbt dependencies separate from Airflow.
RUN python -m venv /opt/airflow/dbt_venv \
    && PIP_USER=false /opt/airflow/dbt_venv/bin/python -m pip install \
        --no-cache-dir dbt

# Keep extraction/loading dependencies separate from Airflow and dbt.
RUN python -m venv /opt/airflow/etl_venv \
    && PIP_USER=false /opt/airflow/etl_venv/bin/python -m pip install \
        --no-cache-dir \
        "snowflake-connector-python[pandas]" \
        pandas \
        openpyxl