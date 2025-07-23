WITH orders AS (
    SELECT 
        *
    FROM {{ source('order_gcp_new_source', 'raw_orders')}} 
)
SELECT
    id AS order_id,
    customer AS customer_id,
    ordered_at,
    store_id,
    subtotal/100.0 AS subtotal,
    tax_paid/100.0 AS tax_paid,
    order_total/100.0 AS order_total
FROM orders