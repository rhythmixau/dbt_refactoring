{{
    config(
        materialized='table'
    )
}}
WITH order_source AS (
    SELECT 
        id,
        user_id,
        order_date,
        status
    FROM
        {{ source('order_gcp_source', 'orders') }}
    WHERE status != 'pending'
)
SELECT
    id AS order_id,
    user_id AS customer_id,
    order_date,
    status AS order_status,
    CASE 
        WHEN status NOT IN ('returned', 'return_pending')
        THEN order_date
    END AS valid_order_date,
    ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY order_date, id
    ) AS user_order_seq
FROM order_source