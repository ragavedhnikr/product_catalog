import os

import snowflake.connector


CSV_PATH = "/opt/airflow/data/batches/products_v1.csv"
TABLE_NAME = "PRODUCT_CATALOG.RAW.RAW_PRODUCTS"
STAGE_NAME = "PRODUCT_CATALOG.RAW.PRODUCT_STAGE"
FILE_FORMAT = "PRODUCT_CATALOG.RAW.CSV_FMT"


connection = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
)

cursor = connection.cursor()

try:
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    existing_rows = cursor.fetchone()[0]

    if existing_rows > 0:
        raise RuntimeError(
            f"{TABLE_NAME} already contains {existing_rows} rows. "
            "This initial-load script will not overwrite it."
        )

    cursor.execute(
        f"""
        PUT file://{CSV_PATH}
        @{STAGE_NAME}
        AUTO_COMPRESS=TRUE
        OVERWRITE=TRUE
        """
    )

    cursor.execute(
        f"""
        COPY INTO {TABLE_NAME} (
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
                'products_v1',
                CURRENT_TIMESTAMP()
            FROM @{STAGE_NAME}
        )
        FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
        PATTERN = '.*products_v1[.]csv[.]gz'
        ON_ERROR = 'ABORT_STATEMENT'
        """
    )

    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    loaded_rows = cursor.fetchone()[0]

    print(f"Loaded rows: {loaded_rows}")

finally:
    cursor.close()
    connection.close()