WITH customers AS (
    SELECT customer_id, full_name FROM {{ ref('stg_new_customers')}}
),
orders AS (
    SELECT order_id, customer_id, ordered_at, subtotal, tax_paid, order_total 
    FROM {{ ref('stg_new_orders') }}
),
mod_customers AS (
SELECT
    customer_id,
    full_name,
    SPLIT(full_name, ' ')[0] AS first_name,
    SPLIT(full_name, ' ')[1] AS last_name
FROM customers
),
customer_orders_summary AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS count_lifetime_orders,
        SUM(subtotal) AS lifetime_spend_pretax,
        SUM(order_total) AS lifet_spend
    FROM orders
    GROUP BY customer_id
),
order_ranks AS (
    SELECT 
        customer_id,
        ordered_at,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ordered_at ASC) AS first_order_rank,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ordered_at DESC) AS recent_order_rank
    FROM orders
),
customer_first_order AS (
    SELECT 
        customer_id,
        ordered_at AS first_order_date
    FROM order_ranks
    WHERE first_order_rank = 1
),
customer_recent_order AS (
    SELECT 
        customer_id,
        ordered_at AS recent_order_date       
    FROM order_ranks
    WHERE recent_order_rank = 1
)
SELECT
    mc.*,
    s.count_lifetime_orders,
    ROUND(s.lifetime_spend_pretax, 2) AS lifetime_spend_pretax,
    ROUND(s.lifet_spend, 2) AS lifetime_spend,
    f.first_order_date,
    r.recent_order_date
FROM mod_customers mc 
JOIN customer_orders_summary s ON s.customer_id = mc.customer_id
JOIN customer_first_order f ON f.customer_id = mc.customer_id
JOIN customer_recent_order r ON r.customer_id = mc.customer_id
