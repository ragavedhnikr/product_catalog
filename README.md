# E-commerce Product Catalog Pipeline

An end-to-end product-catalog data platform project using Python, Apache Airflow, dbt Core, Docker Compose, and Snowflake.

The project demonstrates:

- source validation and batch loading into a Snowflake RAW table;
- dbt staging, testing, snapshots, and an SCD Type 2 product dimension;
- Airflow orchestration with retries, timeouts, dependency ordering, and failure callbacks;
- Snowflake zero-copy cloning for development, point-in-time analysis, prototyping, backup, and restore;
- dated daily backups with seven-day retention and row-count verification.

## Architecture

~~~text
Local workbook / generated batches
              |
              v
Python preparation and validation
              |
              v
Airflow ETL DAG
  validate -> load -> dbt run/test -> snapshot -> SCD2 mart
              |
              v
Snowflake PRODUCT_CATALOG
  RAW -> STAGING -> SNAPSHOTS -> CORE
              |
              +--> DEV / PIT / PROTOTYPE clones
              +--> dated backups -> RESTORED
~~~

## Repository layout

~~~text
product_catalog/
├── dags/
│   ├── product_catalog_etl.py
│   ├── product_catalog_clone_dag.py
│   └── product_catalog_backup_dag.py
├── scripts/
│   ├── prepare_batches.py
│   ├── validate_batch.py
│   ├── load_products_batch.py
│   └── clone_actions.py
├── dbt/product_catalog/
│   ├── models/staging/stg_products.sql
│   ├── models/marts/dim_products_history.sql
│   ├── models/dev/product_rating_experiment.sql
│   ├── snapshots/products_snapshot.sql
│   └── tests/
├── config/profiles.yml
├── evidence/
├── data/
├── docker-compose.yaml
├── Dockerfile
└── snowflake_queries.sql
~~~

## Prerequisites

- Docker Desktop with Docker Compose
- Snowflake account permissions to create warehouses, databases, schemas, stages, tables, and clones
- Git
- Source workbook at **data/original/output.xlsx**

The Docker image supplies Airflow, the ETL environment, and the dbt environment. A local Python or dbt installation is optional.

## Configure credentials

Create a private environment file:

~~~bash
cp .env.example .env
~~~

Set these values in **.env**:

~~~dotenv
SNOWFLAKE_ACCOUNT=<account_identifier>
SNOWFLAKE_USER=<username>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_ROLE=<role>
SNOWFLAKE_WAREHOUSE=PRODUCT_WH
SNOWFLAKE_DATABASE=PRODUCT_CATALOG
SNOWFLAKE_SCHEMA=RAW
~~~

Never commit **.env**. It is ignored by **.gitignore**; commit only **.env.example** with blank or placeholder values.

## Prepare source batches

Place the workbook at **data/original/output.xlsx**, start the stack, then run the preparation script in the worker image:

~~~bash
docker compose run --rm --no-deps --entrypoint bash airflow-worker -lc '
  /opt/airflow/etl_venv/bin/python /opt/airflow/scripts/prepare_batches.py
'
~~~

This creates:

- **products_v1.csv**: initial deduplicated population;
- **products_v2.csv**: deliberate price, availability, category, and new-product changes;
- **products_v3.csv**: unchanged copy of v2 for idempotency testing.

Generated batches are ignored by Git. Validate a batch in the worker image with:

~~~bash
docker compose run --rm --no-deps --entrypoint bash airflow-worker -lc '
  /opt/airflow/etl_venv/bin/python /opt/airflow/scripts/validate_batch.py
    --file products_v3.csv
'
~~~

## Initialize Snowflake

Run **snowflake_queries.sql** or **evidence/03_snowflake_objects.sql** in Snowflake. The setup creates:

- warehouse **PRODUCT_WH**;
- database **PRODUCT_CATALOG**;
- schemas **RAW**, **STAGING**, **SNAPSHOTS**, and **CORE**;
- internal stage **RAW.PRODUCT_STAGE**;
- CSV file format **RAW.CSV_FMT**;
- staging table **RAW.RAW_PRODUCTS**.

## Start Airflow

~~~bash
docker compose config
docker compose up -d --build
docker compose ps
~~~

Open the Airflow UI at http://localhost:8080. Diagnostics:

~~~bash
docker compose logs --tail=100 airflow-dag-processor
docker compose exec airflow-dag-processor airflow dags list-import-errors
docker compose exec airflow-dag-processor airflow dags list
~~~

## Run the ETL DAG

Trigger the controlled source states in order:

~~~bash
docker compose exec airflow-worker airflow dags trigger product_catalog_etl \
  --conf '{"source_file":"products_v1.csv","batch_id":"products_v1"}'

docker compose exec airflow-worker airflow dags trigger product_catalog_etl \
  --conf '{"source_file":"products_v2.csv","batch_id":"products_v2"}'

docker compose exec airflow-worker airflow dags trigger product_catalog_etl \
  --conf '{"source_file":"products_v3.csv","batch_id":"products_v3"}'
~~~

The task chain is:

~~~text
extract_validate -> load_raw_products -> dbt_run_staging
-> dbt_test_staging -> dbt_snapshot -> dbt_run_dimension
-> dbt_test_all
~~~

The DAG uses Airflow **BashOperator** to invoke dbt Core in **/opt/airflow/dbt_venv**. Airflow still controls ordering, retries, logs, timeout, and failure propagation.

## Run dbt directly

~~~bash
docker compose run --rm --no-deps --entrypoint bash airflow-worker -lc '
  cd /opt/airflow/dbt/product_catalog &&
  /opt/airflow/dbt_venv/bin/dbt parse \
    --target dev --profiles-dir /opt/airflow/config
'
~~~

Useful commands are **dbt run**, **dbt test**, and **dbt snapshot** with:

- project directory **/opt/airflow/dbt/product_catalog**;
- profiles directory **/opt/airflow/config**;
- target **dev** or **dev_clone**.

## SCD Type 2 verification

The snapshot tracks descriptive product attributes using **product_id** as the unique key. The core model exposes **product_version_key**, **valid_from**, **valid_to**, and **is_current**.

~~~sql
SELECT product_id, price, valid_from, valid_to, is_current
FROM PRODUCT_CATALOG.CORE.DIM_PRODUCTS_HISTORY
ORDER BY product_id, valid_from;
~~~

The v1 to v2 run should close old rows and create current replacement rows. The unchanged v3 run tests idempotency and should not add another version for unchanged products. See **evidence/06_scd2_verification.sql**.

## Zero-copy cloning

Trigger the clone demonstration:

~~~bash
docker compose exec airflow-worker airflow dags trigger product_catalog_clone_dag
~~~

The chain is:

~~~text
create_dev -> verify_dev -> run_dev_feature -> test_isolation
-> create_pit -> verify_pit -> create_prototype
-> create_backup -> cleanup_backups -> restore_latest -> verify_all
~~~

The supported clone actions are **create_dev**, **verify_dev**, **test_isolation**, **create_prototype**, **create_pit**, **verify_pit**, **create_backup**, **cleanup_backups**, **restore_latest**, **create_restore** (compatibility alias), and **verify_all**.

The PIT operation uses **AT (OFFSET => -60)**. If Time Travel retention or database age does not support the offset, the script reports the limitation and the regular clone tasks can continue.

## Daily backup and restore

The independent backup DAG is scheduled with **@daily**:

~~~bash
docker compose exec airflow-worker airflow dags trigger product_catalog_backup_dag
~~~

Its chain is:

~~~text
create_daily_backup -> cleanup_backups -> restore_latest -> verify_backup_restore
~~~

Each run creates a dated database such as **PRODUCT_CATALOG_BACKUP_20261124**, keeps only the newest seven matching backup databases, restores the newest backup as **PRODUCT_CATALOG_RESTORED**, and compares RAW row counts. A single permanent backup database is not used as the retention strategy.

## Evidence and report

The **evidence/** directory contains environment, dataset, Snowflake, dbt, SCD2, clone, backup, takeaways, and extension evidence. Capture Airflow and Snowflake screenshots after successful runs, and remove credentials from screenshots before publishing them.

## Troubleshooting

### Missing Snowflake environment variables

If a task raises **KeyError: SNOWFLAKE_ACCOUNT**, confirm **.env** exists, contains all required values, and recreate the services:

~~~bash
docker compose up -d --force-recreate
docker compose exec airflow-worker env | grep '^SNOWFLAKE_'
~~~

### DAG not listed

~~~bash
docker compose logs --tail=100 airflow-dag-processor
docker compose exec airflow-dag-processor airflow dags list-import-errors
~~~

### PIT clone unavailable

This is expected when the source database is younger than the requested offset or the Time Travel retention window has expired. Document the limitation, then continue with development, prototype, backup, restore, and verification tasks.

## Cleanup

Stop the local stack:

~~~bash
docker compose down
~~~

Use **docker compose down -v** only when you intentionally want to remove local Airflow metadata and Postgres/Redis volumes. Drop Snowflake development, PIT, restored, and obsolete backup objects according to your retention policy.

## Security

- Never commit **.env**, passwords, private keys, or account secrets.
- Use least-privilege Snowflake roles for shared or production deployments.
- Rotate credentials if they appear in terminal history or screenshots.
- Review all evidence images before publishing to GitHub.
