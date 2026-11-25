SELECT
    product_id,
    price,
    valid_from,
    valid_to,
    is_current
FROM PRODUCT_CATALOG.CORE.DIM_PRODUCTS_HISTORY
WHERE product_id IN (
    SELECT product_id
    FROM PRODUCT_CATALOG.SNAPSHOTS.PRODUCTS_SNAPSHOT
)
ORDER BY product_id, valid_from;

-- to find products with multiple versions
SELECT
    product_id,
    COUNT(*) AS version_count
FROM PRODUCT_CATALOG.CORE.DIM_PRODUCTS_HISTORY
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC
LIMIT 10;