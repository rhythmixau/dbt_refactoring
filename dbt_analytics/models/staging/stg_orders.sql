SELECT 
    id AS order_id,
    user_id,
    order_date,
    status AS order_status
FROM
    {{ source('order_gcp_source', 'orders') }}
WHERE status != 'pending'