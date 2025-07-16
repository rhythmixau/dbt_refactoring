WITH orders AS (
    SELECT * FROM {{ ref("stg_orders") }}
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
    SELECT * FROM {{ ref('stg_payments') }}
), customer_purchase_history AS (
    SELECT * FROM {{ ref('customer_order_history') }}
), final AS (
    select 
    orders.order_id,
    orders.user_id as customer_id,
    full_name,
    surname,
    givenname,
    first_order_date,
    order_count,
    total_lifetime_value,
    round(payment_amount/100.0,2) as order_value_dollars,
    orders.order_status,
    payments.payment_status
from orders 
join customers on orders.user_id = customers.customer_id
join customer_purchase_history h on orders.user_id = h.customer_id
left outer join payments on orders.order_id = payments.order_id
where payments.payment_status != 'fail'
)
SELECT * FROM final