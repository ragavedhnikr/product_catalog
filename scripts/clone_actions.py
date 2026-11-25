# import argparse
# import os

# import snowflake.connector
# import re
# from datetime import datetime, timezone

# SOURCE_DB = "PRODUCT_CATALOG"
# DEV_DB = "PRODUCT_CATALOG_DEV"
# PIT_DB = "PRODUCT_CATALOG_PIT"
# RESTORED_DB = "PRODUCT_CATALOG_RESTORED"
# BACKUP_PREFIX = "PRODUCT_CATALOG_BACKUP_"
# MARKER_ID = "CLONE_ONLY_TEST"

# BACKUP_PATTERN = re.compile(
#     rf"^{BACKUP_PREFIX}(\d{{8}})$"
# )

# def connect():
#     return snowflake.connector.connect(
#         account=os.environ["SNOWFLAKE_ACCOUNT"],
#         user=os.environ["SNOWFLAKE_USER"],
#         password=os.environ["SNOWFLAKE_PASSWORD"],
#         role=os.environ["SNOWFLAKE_ROLE"],
#         warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
#         database="PRODUCT_CATALOG",
#         schema="RAW",
#     )

# def scalar(cur, sql):
#     cur.execute(sql)
#     row = cur.fetchone()
#     return row[0] if row else None


# def database_names(cur):
#     cur.execute("SHOW DATABASES")
#     return [str(row[1]).upper() for row in cur.fetchall()]


# def backup_names(cur):
#     names = []

#     for name in database_names(cur):
#         if BACKUP_PATTERN.fullmatch(name):
#             names.append(name)

#     return sorted(names, reverse=True)

# def execute(sql):
#     print(f"Executing:\n{sql}")

#     connection = connect()

#     try:
#         cursor = connection.cursor()
#         cursor.execute(sql)
#         connection.commit()

#         if cursor.description:
#             for row in cursor.fetchall():
#                 print(row)

#     finally:
#         cursor.close()
#         connection.close()


# def query_one(sql):
#     connection = connect()

#     try:
#         cursor = connection.cursor()
#         cursor.execute(sql)
#         return cursor.fetchone()[0]

#     finally:
#         cursor.close()
#         connection.close()

# def create_pit(cur, args):
#     try:
#         cur.execute(f"""
#             CREATE OR REPLACE DATABASE {PIT_DB}
#             CLONE {SOURCE_DB}
#             AT (OFFSET => -60)
#         """)
#         print(f"Created point-in-time clone: {PIT_DB}")

#     except Exception as exc:
#         message = str(exc)
#         lower_message = message.lower()

#         expected_terms = [
#             "retention",
#             "time travel",
#             "does not exist",
#             "before the point",
#             "at or before",
#         ]

#         if any(term in lower_message for term in expected_terms):
#             print("PIT_LIMITATION")
#             print(message)
#             print("Continuing with the regular clone tasks.")
#             return

#         raise


# def verify_pit(cur, args):
#     if PIT_DB not in database_names(cur):
#         print("PIT clone is unavailable; verification skipped.")
#         return

#     count = scalar(
#         cur,
#         f"SELECT COUNT(*) FROM {PIT_DB}.RAW.RAW_PRODUCTS"
#     )

#     print(f"{PIT_DB}.RAW.RAW_PRODUCTS row count: {count}")


# def create_backup(cur, args):
#     backup_date = args.backup_date

#     if not backup_date:
#         backup_date = datetime.now(timezone.utc).strftime("%Y%m%d")

#     if not re.fullmatch(r"\d{8}", backup_date):
#         raise ValueError("backup-date must use YYYYMMDD format")

#     backup_db = f"{BACKUP_PREFIX}{backup_date}"

#     cur.execute(f"""
#         CREATE OR REPLACE DATABASE {backup_db}
#         CLONE {SOURCE_DB}
#     """)

#     print(f"Created backup: {backup_db}")


# def cleanup_backups(cur, args):
#     backups = backup_names(cur)
#     keep = backups[:7]
#     remove = backups[7:]

#     print(f"Keeping backups: {keep}")

#     for database_name in remove:
#         cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
#         print(f"Dropped old backup: {database_name}")


# def restore_latest(cur, args):
#     backups = backup_names(cur)

#     if not backups:
#         raise RuntimeError("No dated backup databases were found")

#     latest_backup = backups[0]

#     cur.execute(f"""
#         CREATE OR REPLACE DATABASE {RESTORED_DB}
#         CLONE {latest_backup}
#     """)

#     print(f"Restored {latest_backup} as {RESTORED_DB}")

# parser = argparse.ArgumentParser()
# parser.add_argument(
#     "--action",
#     required=True,
#     choices=[
#         "create_dev",
#         "verify_dev",
#         "test_isolation",
#         "create_prototype",
#         "create_backup",
#         "create_restore",
#         "verify_all",
#     ],
# )

# parser.add_argument(
#     "--backup-date",
#     help="Backup date in YYYYMMDD format",
#     choices=[
#         "create_dev",
#         "verify_dev",
#         "test_isolation",
#         "create_prototype",
#         "create_backup",
#         "create_restore",
#         "create_pit",
#         "verify_pit",
#         "cleanup_backups",
#         "restore_latest",
#         "verify_all",
#     ],
# )

# args = parser.parse_args()

# if args.action == "create_dev":
#     execute(
#         """
#         CREATE OR REPLACE DATABASE PRODUCT_CATALOG_DEV
#         CLONE PRODUCT_CATALOG
#         """
#     )

# elif args.action == "verify_dev":
#     production_count = query_one(
#         """
#         SELECT COUNT(*)
#         FROM PRODUCT_CATALOG.RAW.RAW_PRODUCTS
#         """
#     )

#     clone_count = query_one(
#         """
#         SELECT COUNT(*)
#         FROM PRODUCT_CATALOG_DEV.RAW.RAW_PRODUCTS
#         """
#     )

#     print(f"Production raw rows: {production_count}")
#     print(f"Clone raw rows: {clone_count}")

#     if production_count != clone_count:
#         raise RuntimeError("Production and clone row counts do not match.")

# elif args.action == "test_isolation":
#     execute(
#         """
#         INSERT INTO PRODUCT_CATALOG_DEV.RAW.RAW_PRODUCTS (
#             PRODUCT_ID,
#             NAME,
#             BATCH_ID,
#             LOADED_AT
#         )
#         VALUES (
#             'CLONE_ONLY_TEST',
#             'Clone-only test product',
#             'clone_demo',
#             CURRENT_TIMESTAMP()
#         )
#         """
#     )

#     clone_marker_count = query_one(
#         """
#         SELECT COUNT(*)
#         FROM PRODUCT_CATALOG_DEV.RAW.RAW_PRODUCTS
#         WHERE PRODUCT_ID = 'CLONE_ONLY_TEST'
#         """
#     )

#     production_marker_count = query_one(
#         """
#         SELECT COUNT(*)
#         FROM PRODUCT_CATALOG.RAW.RAW_PRODUCTS
#         WHERE PRODUCT_ID = 'CLONE_ONLY_TEST'
#         """
#     )

#     print(f"Marker rows in clone: {clone_marker_count}")
#     print(f"Marker rows in production: {production_marker_count}")

#     if clone_marker_count != 1 or production_marker_count != 0:
#         raise RuntimeError("Clone isolation test failed.")

# elif args.action == "create_prototype":
#     execute(
#         """
#         CREATE OR REPLACE SCHEMA PRODUCT_CATALOG_DEV.PROTOTYPE
#         CLONE PRODUCT_CATALOG.CORE
#         """
#     )

# elif args.action == "create_backup":
#     execute(
#         """
#         CREATE OR REPLACE DATABASE PRODUCT_CATALOG_BACKUP
#         CLONE PRODUCT_CATALOG
#         """
#     )

# elif args.action == "create_restore":
#     execute(
#         """
#         CREATE OR REPLACE DATABASE PRODUCT_CATALOG_RESTORED
#         CLONE PRODUCT_CATALOG_BACKUP
#         """
#     )

# elif args.action == "verify_all":
#     execute(
#         """
#         SHOW DATABASES LIKE 'PRODUCT_CATALOG%';
#         """
#     )

#     execute(
#         """
#         SHOW TABLES IN SCHEMA PRODUCT_CATALOG_DEV.PROTOTYPE;
#         """
#     )

# elif args.action == "create_pit":
#     create_pit(cur, args)

# elif args.action == "verify_pit":
#     verify_pit(cur, args)

# elif args.action == "cleanup_backups":
#     cleanup_backups(cur, args)

# elif args.action == "restore_latest":
#     restore_latest(cur, args)

# elif args.action == "create_restore":
#     # Backward-compatible alias
#     restore_latest(cur, args)

"""Snowflake zero-copy clone actions used by the product-catalog Airflow DAG."""

from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timezone

import snowflake.connector


SOURCE_DB = "PRODUCT_CATALOG"
DEV_DB = "PRODUCT_CATALOG_DEV"
PIT_DB = "PRODUCT_CATALOG_PIT"
RESTORED_DB = "PRODUCT_CATALOG_RESTORED"
STATIC_BACKUP_DB = "PRODUCT_CATALOG_BACKUP"
BACKUP_PREFIX = "PRODUCT_CATALOG_BACKUP_"
BACKUP_PATTERN = re.compile(r"^PRODUCT_CATALOG_BACKUP_(\d{8})$")
MARKER_ID = "CLONE_ONLY_TEST"


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=SOURCE_DB,
        schema="RAW",
    )


def execute(sql, params=None):
    print(f"Executing:\n{sql.strip()}")
    connection = connect()
    cursor = None

    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        connection.commit()

        if cursor.description:
            rows = cursor.fetchall()
            for row in rows:
                print(row)
            return rows

        return []
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()


def query_one(sql, params=None):
    connection = connect()
    cursor = None

    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError(f"Query returned no rows: {sql}")
        return row[0]
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()


def query_all(sql, params=None):
    connection = connect()
    cursor = None

    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        return cursor.fetchall()
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()


def database_names():
    # SHOW DATABASES returns the database name in column 2 (index 1).
    return [str(row[1]).upper() for row in query_all("SHOW DATABASES")]


def backup_names():
    names = [name for name in database_names() if BACKUP_PATTERN.fullmatch(name)]
    return sorted(names, reverse=True)


def create_dev():
    execute(f"CREATE OR REPLACE DATABASE {DEV_DB} CLONE {SOURCE_DB}")
    print(f"Created development clone: {DEV_DB}")


def verify_dev():
    production_count = query_one(
        f"SELECT COUNT(*) FROM {SOURCE_DB}.RAW.RAW_PRODUCTS"
    )
    clone_count = query_one(
        f"SELECT COUNT(*) FROM {DEV_DB}.RAW.RAW_PRODUCTS"
    )

    print(f"Production raw rows: {production_count}")
    print(f"Clone raw rows: {clone_count}")

    if production_count != clone_count:
        raise RuntimeError("Production and clone row counts do not match.")


def test_isolation():
    delete_marker = f"""
        DELETE FROM {DEV_DB}.RAW.RAW_PRODUCTS
        WHERE PRODUCT_ID = %s
    """

    insert_marker = f"""
        INSERT INTO {DEV_DB}.RAW.RAW_PRODUCTS (
            PRODUCT_ID,
            NAME,
            BATCH_ID,
            LOADED_AT
        )
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP())
    """

    execute(delete_marker, (MARKER_ID,))

    try:
        execute(
            insert_marker,
            (MARKER_ID, "Clone-only test product", "clone_demo"),
        )

        clone_marker_count = query_one(
            f"""
            SELECT COUNT(*)
            FROM {DEV_DB}.RAW.RAW_PRODUCTS
            WHERE PRODUCT_ID = %s
            """,
            (MARKER_ID,),
        )
        production_marker_count = query_one(
            f"""
            SELECT COUNT(*)
            FROM {SOURCE_DB}.RAW.RAW_PRODUCTS
            WHERE PRODUCT_ID = %s
            """,
            (MARKER_ID,),
        )

        print(f"Marker rows in clone: {clone_marker_count}")
        print(f"Marker rows in production: {production_marker_count}")

        if clone_marker_count != 1 or production_marker_count != 0:
            raise RuntimeError("Clone isolation test failed.")
    finally:
        # Keep the test idempotent and do not leave test data behind.
        execute(delete_marker, (MARKER_ID,))


def create_prototype():
    execute(
        f"""
        CREATE OR REPLACE SCHEMA {DEV_DB}.PROTOTYPE
        CLONE {DEV_DB}.CORE
        """
    )
    print(f"Created prototype schema: {DEV_DB}.PROTOTYPE")


def create_pit():
    try:
        execute(
            f"""
            CREATE OR REPLACE DATABASE {PIT_DB}
            CLONE {SOURCE_DB}
            AT (OFFSET => -60)
            """
        )
        print(f"Created point-in-time clone: {PIT_DB}")
    except Exception as exc:
        message = str(exc)
        lower_message = message.lower()
        expected_terms = (
            "retention",
            "time travel",
            "does not exist",
            "before the point",
            "at or before",
        )

        if any(term in lower_message for term in expected_terms):
            print("PIT_LIMITATION")
            print(message)
            print("Continuing with the regular clone tasks.")
            return

        raise


def verify_pit():
    if PIT_DB not in database_names():
        print("PIT clone is unavailable; verification skipped.")
        return

    pit_count = query_one(f"SELECT COUNT(*) FROM {PIT_DB}.RAW.RAW_PRODUCTS")
    print(f"{PIT_DB}.RAW.RAW_PRODUCTS row count: {pit_count}")

    try:
        historical_count = query_one(
            f"""
            SELECT COUNT(*)
            FROM {SOURCE_DB}.RAW.RAW_PRODUCTS
            AT (OFFSET => -60)
            """
        )
        print(f"Source-at-offset row count: {historical_count}")
    except Exception as exc:
        print("Historical source query unavailable:")
        print(exc)


def create_backup(backup_date=None):
    backup_date = backup_date or datetime.now(timezone.utc).strftime("%Y%m%d")

    if not re.fullmatch(r"\d{8}", backup_date):
        raise ValueError("backup-date must use YYYYMMDD format")

    backup_db = f"{BACKUP_PREFIX}{backup_date}"
    execute(f"CREATE OR REPLACE DATABASE {backup_db} CLONE {SOURCE_DB}")
    print(f"Created backup: {backup_db}")


def cleanup_backups():
    backups = backup_names()
    keep = backups[:7]
    remove = backups[7:]

    print(f"Keeping backups: {keep}")

    for backup_db in remove:
        execute(f"DROP DATABASE IF EXISTS {backup_db}")
        print(f"Dropped old backup: {backup_db}")


def restore_latest():
    backups = backup_names()

    if backups:
        source_backup = backups[0]
    elif STATIC_BACKUP_DB in database_names():
        # Preserve compatibility with the original script.
        source_backup = STATIC_BACKUP_DB
    else:
        raise RuntimeError("No backup database was found")

    execute(
        f"CREATE OR REPLACE DATABASE {RESTORED_DB} CLONE {source_backup}"
    )
    print(f"Restored {source_backup} as {RESTORED_DB}")


def verify_all():
    print("Databases:")
    for row in query_all("SHOW DATABASES LIKE 'PRODUCT_CATALOG%'"):
        print(row)

    if DEV_DB in database_names():
        production_count = query_one(
            f"SELECT COUNT(*) FROM {SOURCE_DB}.RAW.RAW_PRODUCTS"
        )
        clone_count = query_one(
            f"""
            SELECT COUNT(*)
            FROM {DEV_DB}.RAW.RAW_PRODUCTS
            WHERE PRODUCT_ID <> %s
            """,
            (MARKER_ID,),
        )

        if production_count != clone_count:
            raise RuntimeError("Final production/dev counts do not match")

        print(f"Final production/dev count: {production_count}")

    prototype_tables = query_all(
        f"SHOW TABLES IN SCHEMA {DEV_DB}.PROTOTYPE"
    )
    if not prototype_tables:
        raise RuntimeError("Prototype schema contains no tables")
    print(f"Prototype tables: {len(prototype_tables)}")

    backups = backup_names()
    if backups:
        latest_backup = backups[0]
        backup_count = query_one(
            f"SELECT COUNT(*) FROM {latest_backup}.RAW.RAW_PRODUCTS"
        )
        restored_count = query_one(
            f"SELECT COUNT(*) FROM {RESTORED_DB}.RAW.RAW_PRODUCTS"
        )

        print(f"Latest backup: {latest_backup}")
        print(f"Backup rows: {backup_count}")
        print(f"Restored rows: {restored_count}")

        if backup_count != restored_count:
            raise RuntimeError("Backup and restored row counts do not match")

    if PIT_DB in database_names():
        pit_count = query_one(f"SELECT COUNT(*) FROM {PIT_DB}.RAW.RAW_PRODUCTS")
        print(f"PIT rows: {pit_count}")
    else:
        print("PIT database not present; regular clone verification succeeded")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--action",
        required=True,
        choices=[
            "create_dev",
            "verify_dev",
            "test_isolation",
            "create_prototype",
            "create_pit",
            "verify_pit",
            "create_backup",
            "cleanup_backups",
            "restore_latest",
            "create_restore",  # Backward-compatible alias.
            "verify_all",
        ],
    )
    parser.add_argument("--backup-date")
    args = parser.parse_args()

    actions = {
        "create_dev": create_dev,
        "verify_dev": verify_dev,
        "test_isolation": test_isolation,
        "create_prototype": create_prototype,
        "create_pit": create_pit,
        "verify_pit": verify_pit,
        "create_backup": lambda: create_backup(args.backup_date),
        "cleanup_backups": cleanup_backups,
        "restore_latest": restore_latest,
        "create_restore": restore_latest,
        "verify_all": verify_all,
    }

    actions[args.action]()


if __name__ == "__main__":
    main()
