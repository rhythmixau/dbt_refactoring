WITH orders AS (
    SELECT order_id, 
    customer_id, 
    order_status 
    FROM {{ ref("stg_orders") }}
), 
customers AS (
    SELECT 
        customer_id,
        (first_name || ' ' ||  last_name) AS full_name,
        last_name as surname,
        first_name as givenname,
    FROM {{ ref('stg_customers') }}
),
payments AS (
    SELECT 
        payment_id, 
        payment_status, 
        order_id,
        payment_amount
    FROM {{ ref('stg_payments') }}
    where payment_status != 'fail'
), customer_purchase_history AS (
    SELECT * FROM {{ ref('customer_order_history') }}
), 
payment_totals AS (
    SELECT 
        order_id, 
        payment_status,
        sum(payment_amount) AS order_value
    FROM payments
    GROUP BY order_id, payment_status
),
order_values AS (
    SELECT 
        o.order_id, 
        o.customer_id,
        ROUND(p.order_value, 2) order_value_dollars,
        o.order_status,
        p.payment_status
    FROM orders o
    JOIN payment_totals p ON p.order_id = o.order_id
),
final AS (
    select 
    v.order_id,
    v.customer_id,
    c.full_name,
    c.surname,
    c.givenname,
    h.first_order_date,
    h.order_count,
    h.total_lifetime_value,
    v.order_value_dollars,
    v.order_status,
    v.payment_status
from order_values v
join customers c on v.customer_id = c.customer_id
join customer_purchase_history h on v.customer_id = h.customer_id
)
SELECT * FROM final