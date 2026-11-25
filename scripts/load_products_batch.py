import argparse
import os
from pathlib import Path

import snowflake.connector


parser = argparse.ArgumentParser()
parser.add_argument("--file", required=True)
parser.add_argument("--batch-id", required=True)
args = parser.parse_args()

allowed_files = {
    "products_v1.csv",
    "products_v2.csv",
    "products_v3.csv",
}

if args.file not in allowed_files:
    raise ValueError(f"Unsupported file: {args.file}")

csv_path = Path("/opt/airflow/data/batches") / args.file
stage_file = f"{args.file}.gz"

connection = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema="RAW",
)

cursor = connection.cursor()

try:
    cursor.execute("USE DATABASE PRODUCT_CATALOG")
    cursor.execute("USE SCHEMA RAW")

    cursor.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE LOAD_PRODUCTS
        LIKE RAW_PRODUCTS
        """
    )

    cursor.execute(
        f"""
        PUT file://{csv_path}
        @PRODUCT_STAGE
        AUTO_COMPRESS=TRUE
        OVERWRITE=TRUE
        """
    )

    cursor.execute(
        f"""
        COPY INTO LOAD_PRODUCTS (
            PRODUCT_ID,
            NAME,
            BRAND,
            CATEGORY,
            SUB_CATEGORY,
            PRICE,
            LIST_PRICE,
            DISCOUNT_PCT,
            AVERAGE_RATING,
            OUT_OF_STOCK,
            DESCRIPTION,
            SELLER,
            PRODUCT_URL,
            SOURCE_CRAWLED_AT,
            SOURCE_FILE,
            BATCH_ID,
            LOADED_AT
        )
        FROM (
            SELECT
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                $11,
                $12,
                $13,
                $14,
                METADATA$FILENAME,
                '{args.batch_id}',
                CURRENT_TIMESTAMP()
            FROM @PRODUCT_STAGE/{stage_file}
        )
        FILE_FORMAT = (
            FORMAT_NAME = 'CSV_FMT'
        )
        ON_ERROR = 'ABORT_STATEMENT'
        """
    )

    cursor.execute("SELECT COUNT(*) FROM LOAD_PRODUCTS")
    load_count = cursor.fetchone()[0]

    if load_count == 0:
        raise RuntimeError("The batch loaded zero rows.")

    cursor.execute("TRUNCATE TABLE RAW_PRODUCTS")

    cursor.execute(
        """
        INSERT INTO RAW_PRODUCTS
        SELECT *
        FROM LOAD_PRODUCTS
        """
    )

    connection.commit()

    cursor.execute("SELECT COUNT(*) FROM RAW_PRODUCTS")
    final_count = cursor.fetchone()[0]

    print(f"Batch: {args.batch_id}")
    print(f"Rows loaded: {final_count}")

finally:
    cursor.close()
    connection.close()