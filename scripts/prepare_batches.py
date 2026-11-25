from pathlib import Path
import pandas as pd

INPUT = Path("/opt/airflow/data/original/output.xlsx")
OUTPUT_DIR = Path("/opt/airflow/data/batches")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

raw = pd.read_excel(INPUT, sheet_name="Sheet1")

raw["_source_row"] = range(len(raw))
raw["pid"] = raw["pid"].astype("string").str.strip()
raw["_crawled_dt"] = pd.to_datetime(
    raw["crawled_at"],
    errors="coerce",
    dayfirst=True,
)

# Reject records without a business key.
raw = raw[raw["pid"].notna() & raw["pid"].ne("")].copy()

# Keep the latest record for each pid.
raw = raw.sort_values(
    ["pid", "_crawled_dt", "_source_row"],
    na_position="first",
)

duplicates_removed = int(raw.duplicated("pid").sum())
raw = raw.drop_duplicates("pid", keep="last").copy()


def parse_money(series):
    cleaned = (
        series.astype("string")
        .str.replace(r"[^0-9.\-]", "", regex=True)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def parse_discount(series):
    return pd.to_numeric(
        series.astype("string")
        .str.extract(r"([0-9]+(?:\.[0-9]+)?)", expand=False),
        errors="coerce",
    )


def parse_boolean(series):
    values = series.astype("string").str.strip().str.lower()
    return values.map(
        {
            "true": "TRUE",
            "false": "FALSE",
            "1": "TRUE",
            "0": "FALSE",
        }
    )


products = pd.DataFrame(
    {
        "product_id": raw["pid"],
        "name": raw["title"].astype("string").str.strip(),
        "brand": raw["brand"].astype("string").str.strip(),
        "category": raw["category"].astype("string").str.strip(),
        "sub_category": raw["sub_category"].astype("string").str.strip(),
        "price": parse_money(raw["selling_price"]),
        "list_price": parse_money(raw["actual_price"]),
        "discount_pct": parse_discount(raw["discount"]),
        "average_rating": pd.to_numeric(
            raw["average_rating"],
            errors="coerce",
        ),
        "out_of_stock": parse_boolean(raw["out_of_stock"]),
        "description": raw["description"].astype("string").str.strip(),
        "seller": raw["seller"].astype("string").str.strip(),
        "product_url": raw["url"].astype("string").str.strip(),
        "source_crawled_at": raw["crawled_at"].astype("string").str.strip(),
    }
)

# Initial source state.
products.to_csv(
    OUTPUT_DIR / "products_v1.csv",
    index=False,
    na_rep="",
)

# Second source state: deliberately modify products.
products_v2 = products.copy()

price_indexes = products_v2.index[
    products_v2["price"].notna()
][:20]

products_v2.loc[price_indexes, "price"] = (
    products_v2.loc[price_indexes, "price"] + 50
).round(2)

availability_indexes = products_v2.index[:5]

products_v2.loc[availability_indexes, "out_of_stock"] = (
    products_v2.loc[availability_indexes, "out_of_stock"]
    .map({"TRUE": "FALSE", "FALSE": "TRUE"})
    .fillna("TRUE")
)

category_indexes = products_v2.index[:3]

products_v2.loc[category_indexes, "category"] = (
    products_v2.loc[category_indexes, "category"].astype("string")
    + " - Updated"
)

# Add one new product.
new_product = pd.DataFrame(
    [
        {
            "product_id": "DEMO-NEW-0001",
            "name": "Demo Product Added in Batch 2",
            "brand": "Demo Brand",
            "category": "Demo",
            "sub_category": "Demo",
            "price": 999.00,
            "list_price": 1299.00,
            "discount_pct": 23.0,
            "average_rating": 4.0,
            "out_of_stock": "FALSE",
            "description": "Synthetic product added for SCD2 testing.",
            "seller": "Demo Seller",
            "product_url": "",
            "source_crawled_at": "",
        }
    ]
)

products_v2 = pd.concat(
    [products_v2, new_product],
    ignore_index=True,
)

products_v2.to_csv(
    OUTPUT_DIR / "products_v2.csv",
    index=False,
    na_rep="",
)

# Third state is intentionally unchanged from batch 2.
products_v2.to_csv(
    OUTPUT_DIR / "products_v3.csv",
    index=False,
    na_rep="",
)

print(f"Original rows: 30000")
print(f"Rows after pid deduplication: {len(products)}")
print(f"Duplicate rows removed: {duplicates_removed}")
print(f"products_v1 rows: {len(products)}")
print(f"products_v2 rows: {len(products_v2)}")
print(f"products_v3 rows: {len(products_v2)}")
print("Created products_v1.csv, products_v2.csv, products_v3.csv")