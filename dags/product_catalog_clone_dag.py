# from datetime import timedelta

# import pendulum

# from airflow.sdk import DAG
# from airflow.providers.standard.operators.bash import BashOperator


# def failure_callback(context):
#     task_instance = context["task_instance"]

#     print(
#         "Clone task failed: "
#         f"{task_instance.dag_id}.{task_instance.task_id}"
#     )


# default_args = {
#     "owner": "product_catalog",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=1),
#     "execution_timeout": timedelta(minutes=10),
#     "on_failure_callback": failure_callback,
# }


# CLONE_COMMAND = (
#     "/opt/airflow/etl_venv/bin/python "
#     "/opt/airflow/scripts/clone_actions.py"
# )


# with DAG(
#     dag_id="product_catalog_clone_dag",
#     start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
#     schedule=None,
#     catchup=False,
#     max_active_runs=1,
#     default_args=default_args,
#     description="Snowflake zero-copy clone demonstrations",
# ) as dag:

#     create_dev_clone = BashOperator(
#         task_id="create_dev_clone",
#         bash_command=(
#             "set -euo pipefail\n"
#             f"{CLONE_COMMAND} --action create_dev"
#         ),
#     )

#     verify_dev_clone = BashOperator(
#         task_id="verify_dev_clone",
#         bash_command=(
#             "set -euo pipefail\n"
#             f"{CLONE_COMMAND} --action verify_dev"
#         ),
#     )

#     test_clone_isolation = BashOperator(
#         task_id="test_clone_isolation",
#         bash_command=(
#             "set -euo pipefail\n"
#             f"{CLONE_COMMAND} --action test_isolation"
#         ),
#     )

#     create_prototype_schema = BashOperator(
#         task_id="create_prototype_schema",
#         bash_command=(
#             "set -euo pipefail\n"
#             f"{CLONE_COMMAND} --action create_prototype"
#         ),
#     )

#     create_backup_clone = BashOperator(
#         task_id="create_backup_clone",
#         bash_command=(
#             "set -euo pipefail\n"
#             f"{CLONE_COMMAND} --action create_backup"
#         ),
#     )

#     create_restore_clone = BashOperator(
#         task_id="create_restore_clone",
#         bash_command=(
#             "set -euo pipefail\n"
#             f"{CLONE_COMMAND} --action create_restore"
#         ),
#     )

#     verify_clone_objects = BashOperator(
#         task_id="verify_clone_objects",
#         bash_command=(
#             "set -euo pipefail\n"
#             f"{CLONE_COMMAND} --action verify_all"
#         ),
#     )

#     (
#         create_dev_clone
#         >> verify_dev_clone
#         >> test_clone_isolation
#         >> create_prototype_schema
#         >> create_backup_clone
#         >> create_restore_clone
#         >> verify_clone_objects
#     )

from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


def failure_callback(context):
    task_instance = context["task_instance"]
    print(
        "Clone task failed: "
        f"{task_instance.dag_id}.{task_instance.task_id}"
    )


default_args = {
    "owner": "product_catalog",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": failure_callback,
}


CLONE_COMMAND = (
    "/opt/airflow/etl_venv/bin/python "
    "/opt/airflow/scripts/clone_actions.py"
)

DBT_COMMAND = (
    "cd /opt/airflow/dbt/product_catalog && "
    "/opt/airflow/dbt_venv/bin/dbt run "
    "--target dev_clone "
    "--profiles-dir /opt/airflow/config "
    "--select product_rating_experiment"
)


with DAG(
    dag_id="product_catalog_clone_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    description="Snowflake zero-copy clone demonstrations",
    tags=["product_catalog", "snowflake", "zero_copy_clone"],
) as dag:

    create_dev = BashOperator(
        task_id="create_dev",
        bash_command=(
            "set -euo pipefail\n"
            f"{CLONE_COMMAND} --action create_dev"
        ),
    )

    verify_dev = BashOperator(
        task_id="verify_dev",
        bash_command=(
            "set -euo pipefail\n"
            f"{CLONE_COMMAND} --action verify_dev"
        ),
    )

    run_dev_feature = BashOperator(
        task_id="run_dev_feature",
        bash_command=(
            "set -euo pipefail\n"
            f"{DBT_COMMAND}"
        ),
    )

    test_isolation = BashOperator(
        task_id="test_isolation",
        bash_command=(
            "set -euo pipefail\n"
            f"{CLONE_COMMAND} --action test_isolation"
        ),
    )

    create_pit = BashOperator(
        task_id="create_pit",
        bash_command=(
            "set -euo pipefail\n"
            f"{CLONE_COMMAND} --action create_pit"
        ),
    )

    verify_pit = BashOperator(
        task_id="verify_pit",
        bash_command=(
            "set -euo pipefail\n"
            f"{CLONE_COMMAND} --action verify_pit"
        ),
    )

    create_prototype = BashOperator(
        task_id="create_prototype",
        bash_command=(
            "set -euo pipefail\n"
            f"{CLONE_COMMAND} --action create_prototype"
        ),
    )

    create_backup = BashOperator(
        task_id="create_backup",
        bash_command=(
            "set -euo pipefail\n"
            f"{CLONE_COMMAND} --action create_backup "
            "--backup-date '{{ ds_nodash }}'"
        ),
    )

    cleanup_backups = BashOperator(
        task_id="cleanup_backups",
        bash_command=(
            "set -euo pipefail\n"
            f"{CLONE_COMMAND} --action cleanup_backups"
        ),
    )

    restore_latest = BashOperator(
        task_id="restore_latest",
        bash_command=(
            "set -euo pipefail\n"
            f"{CLONE_COMMAND} --action restore_latest"
        ),
    )

    verify_all = BashOperator(
        task_id="verify_all",
        bash_command=(
            "set -euo pipefail\n"
            f"{CLONE_COMMAND} --action verify_all"
        ),
    )

    (
        create_dev
        >> verify_dev
        >> run_dev_feature
        >> test_isolation
        >> create_pit
        >> verify_pit
        >> create_prototype
        >> create_backup
        >> cleanup_backups
        >> restore_latest
        >> verify_all
    )
