WITH products AS (
    SELECT * FROM {{ source('order_gcp_new_source', 'raw_products') }}
)
SELECT
    sku AS product_sku,
    name AS product_name,
    type AS product_type,
    price/100.0 AS product_price,
    description AS product_description
FROM products