import argparse
import os

import snowflake.connector


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="PRODUCT_CATALOG",
        schema="RAW",
    )


def execute(sql):
    print(f"Executing:\n{sql}")

    connection = connect()

    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()

        if cursor.description:
            for row in cursor.fetchall():
                print(row)

    finally:
        cursor.close()
        connection.close()


def query_one(sql):
    connection = connect()

    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        return cursor.fetchone()[0]

    finally:
        cursor.close()
        connection.close()


parser = argparse.ArgumentParser()
parser.add_argument(
    "--action",
    required=True,
    choices=[
        "create_dev",
        "verify_dev",
        "test_isolation",
        "create_prototype",
        "create_backup",
        "create_restore",
        "verify_all",
    ],
)

args = parser.parse_args()

if args.action == "create_dev":
    execute(
        """
        CREATE OR REPLACE DATABASE PRODUCT_CATALOG_DEV
        CLONE PRODUCT_CATALOG
        """
    )

elif args.action == "verify_dev":
    production_count = query_one(
        """
        SELECT COUNT(*)
        FROM PRODUCT_CATALOG.RAW.RAW_PRODUCTS
        """
    )

    clone_count = query_one(
        """
        SELECT COUNT(*)
        FROM PRODUCT_CATALOG_DEV.RAW.RAW_PRODUCTS
        """
    )

    print(f"Production raw rows: {production_count}")
    print(f"Clone raw rows: {clone_count}")

    if production_count != clone_count:
        raise RuntimeError("Production and clone row counts do not match.")

elif args.action == "test_isolation":
    execute(
        """
        INSERT INTO PRODUCT_CATALOG_DEV.RAW.RAW_PRODUCTS (
            PRODUCT_ID,
            NAME,
            BATCH_ID,
            LOADED_AT
        )
        VALUES (
            'CLONE_ONLY_TEST',
            'Clone-only test product',
            'clone_demo',
            CURRENT_TIMESTAMP()
        )
        """
    )

    clone_marker_count = query_one(
        """
        SELECT COUNT(*)
        FROM PRODUCT_CATALOG_DEV.RAW.RAW_PRODUCTS
        WHERE PRODUCT_ID = 'CLONE_ONLY_TEST'
        """
    )

    production_marker_count = query_one(
        """
        SELECT COUNT(*)
        FROM PRODUCT_CATALOG.RAW.RAW_PRODUCTS
        WHERE PRODUCT_ID = 'CLONE_ONLY_TEST'
        """
    )

    print(f"Marker rows in clone: {clone_marker_count}")
    print(f"Marker rows in production: {production_marker_count}")

    if clone_marker_count != 1 or production_marker_count != 0:
        raise RuntimeError("Clone isolation test failed.")

elif args.action == "create_prototype":
    execute(
        """
        CREATE OR REPLACE SCHEMA PRODUCT_CATALOG_DEV.PROTOTYPE
        CLONE PRODUCT_CATALOG.CORE
        """
    )

elif args.action == "create_backup":
    execute(
        """
        CREATE OR REPLACE DATABASE PRODUCT_CATALOG_BACKUP
        CLONE PRODUCT_CATALOG
        """
    )

elif args.action == "create_restore":
    execute(
        """
        CREATE OR REPLACE DATABASE PRODUCT_CATALOG_RESTORED
        CLONE PRODUCT_CATALOG_BACKUP
        """
    )

elif args.action == "verify_all":
    execute(
        """
        SHOW DATABASES LIKE 'PRODUCT_CATALOG%';
        """
    )

    execute(
        """
        SHOW TABLES IN SCHEMA PRODUCT_CATALOG_DEV.PROTOTYPE;
        """
    )