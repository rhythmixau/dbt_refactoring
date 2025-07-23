WITH customers AS (
    SELECT * FROM {{ source('order_gcp_new_source', 'raw_customers')}}
)
SELECT 
    id AS customer_id,
    name AS full_name
FROM customers