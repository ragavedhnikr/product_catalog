import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

SCRIPT = "/opt/airflow/scripts/clone_actions.py"
PYTHON = "/opt/airflow/etl_venv/bin/python"

with DAG(
    dag_id="product_catalog_backup_dag",
    start_date=pendulum.datetime(2026, 11, 24, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["product_catalog", "backup"],
) as dag:

    create_daily_backup = BashOperator(
        task_id="create_daily_backup",
        bash_command=(
            "set -euo pipefail\n"
            f"{PYTHON} {SCRIPT} "
            "--action create_backup "
            "--backup-date '{{ ds_nodash }}'"
        ),
    )

    cleanup_backups = BashOperator(
        task_id="cleanup_backups",
        bash_command=(
            "set -euo pipefail\n"
            f"{PYTHON} {SCRIPT} --action cleanup_backups"
        ),
    )

    restore_latest = BashOperator(
        task_id="restore_latest",
        bash_command=(
            "set -euo pipefail\n"
            f"{PYTHON} {SCRIPT} --action restore_latest"
        ),
    )

    verify_backup_restore = BashOperator(
        task_id="verify_backup_restore",
        bash_command=(
            "set -euo pipefail\n"
            f"{PYTHON} {SCRIPT} --action verify_all"
        ),
    )

    (
        create_daily_backup
        >> cleanup_backups
        >> restore_latest
        >> verify_backup_restore
    )