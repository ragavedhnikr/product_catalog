from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


def failure_callback(context):
    task_instance = context["task_instance"]

    print(
        "Task failed: "
        f"{task_instance.dag_id}.{task_instance.task_id}"
    )


default_args = {
    "owner": "product_catalog",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": failure_callback,
}


with DAG(
    dag_id="product_catalog_etl",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "source_file": "products_v3.csv",
        "batch_id": "products_v3",
    },
    description="Product catalog ingestion, dbt transformation, and testing",
) as dag:

    extract_validate = BashOperator(
        task_id="extract_validate",
        bash_command=(
            "set -euo pipefail\n"
            "/opt/airflow/etl_venv/bin/python "
            "/opt/airflow/scripts/validate_batch.py "
            "--file '{{ params.source_file }}'"
        ),
    )

    load_raw_products = BashOperator(
        task_id="load_raw_products",
        bash_command=(
            "set -euo pipefail\n"
            "/opt/airflow/etl_venv/bin/python "
            "/opt/airflow/scripts/load_products_batch.py "
            "--file '{{ params.source_file }}' "
            "--batch-id '{{ params.batch_id }}'"
        ),
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "set -euo pipefail\n"
            "/opt/airflow/dbt_venv/bin/dbt run "
            "--project-dir /opt/airflow/dbt/product_catalog "
            "--profiles-dir /opt/airflow/config "
            "--select stg_products"
        ),
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            "set -euo pipefail\n"
            "/opt/airflow/dbt_venv/bin/dbt test "
            "--project-dir /opt/airflow/dbt/product_catalog "
            "--profiles-dir /opt/airflow/config "
            "--select stg_products"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            "set -euo pipefail\n"
            "/opt/airflow/dbt_venv/bin/dbt snapshot "
            "--project-dir /opt/airflow/dbt/product_catalog "
            "--profiles-dir /opt/airflow/config "
            "--select products_snapshot"
        ),
    )

    dbt_run_dimension = BashOperator(
        task_id="dbt_run_dimension",
        bash_command=(
            "set -euo pipefail\n"
            "/opt/airflow/dbt_venv/bin/dbt run "
            "--project-dir /opt/airflow/dbt/product_catalog "
            "--profiles-dir /opt/airflow/config "
            "--select dim_products_history"
        ),
    )

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=(
            "set -euo pipefail\n"
            "/opt/airflow/dbt_venv/bin/dbt test "
            "--project-dir /opt/airflow/dbt/product_catalog "
            "--profiles-dir /opt/airflow/config"
        ),
    )

    (
        extract_validate
        >> load_raw_products
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_snapshot
        >> dbt_run_dimension
        >> dbt_test_all
    )