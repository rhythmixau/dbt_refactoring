WITH order_items AS (
    SELECT * FROM {{ source('order_gcp_new_source', 'raw_items') }}
)
SELECT
    id AS order_item_id,
    order_id,
    sku
FROM order_items