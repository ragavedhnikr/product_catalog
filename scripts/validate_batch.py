import argparse
from pathlib import Path

import pandas as pd


parser = argparse.ArgumentParser()
parser.add_argument("--file", required=True)
args = parser.parse_args()

allowed_files = {
    "products_v1.csv",
    "products_v2.csv",
    "products_v3.csv",
}

if args.file not in allowed_files:
    raise ValueError(f"Unsupported batch file: {args.file}")

path = Path("/opt/airflow/data/batches") / args.file

if not path.exists():
    raise FileNotFoundError(f"Batch file does not exist: {path}")

df = pd.read_csv(path, dtype={"product_id": "string"})

required_columns = {
    "product_id",
    "name",
    "brand",
    "category",
    "sub_category",
    "price",
    "list_price",
    "discount_pct",
    "average_rating",
    "out_of_stock",
    "description",
    "seller",
    "product_url",
    "source_crawled_at",
}

missing_columns = required_columns - set(df.columns)

if missing_columns:
    raise ValueError(f"Missing columns: {sorted(missing_columns)}")

if df.empty:
    raise ValueError("The batch file is empty.")

if df["product_id"].isna().any():
    raise ValueError("The batch contains null product IDs.")

if df["product_id"].duplicated().any():
    duplicate_count = int(df["product_id"].duplicated().sum())
    raise ValueError(
        f"The batch contains {duplicate_count} duplicate product IDs."
    )

if (df["price"].dropna() < 0).any():
    raise ValueError("The batch contains negative prices.")

print(f"Validated file: {args.file}")
print(f"Rows: {len(df)}")
print(f"Unique product IDs: {df['product_id'].nunique()}")