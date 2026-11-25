from pathlib import Path
import pandas as pd

path = Path("/opt/airflow/data/original/output.xlsx")

df = pd.read_excel(path, sheet_name="Sheet1")

print(f"Rows: {len(df)}")
print(f"Columns: {len(df.columns)}")
print("Column names:")
print(list(df.columns))

print("\nProduct ID checks:")
print("Missing pid:", df["pid"].isna().sum())
print("Duplicate pid:", df["pid"].duplicated().sum())
print("Unique pid:", df["pid"].nunique())

print("\nMissing values in important columns:")
important_columns = [
    "pid",
    "title",
    "brand",
    "category",
    "selling_price",
    "actual_price",
    "out_of_stock",
    "description",
    "crawled_at",
]

for column in important_columns:
    if column in df.columns:
        print(f"{column}: {df[column].isna().sum()}")

print("\nSample product IDs:")
print(df["pid"].head(10).to_list())