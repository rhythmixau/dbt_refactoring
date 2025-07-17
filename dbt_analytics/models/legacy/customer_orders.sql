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
    orders.customer_id,
    customers.full_name,
    customers.surname,
    customers.givenname,
    h.first_order_date,
    h.order_count,
    h.total_lifetime_value,
    round(payments.payment_amount/100.0,2) as order_value_dollars,
    orders.order_status,
    payments.payment_status
from orders 
join customers on orders.customer_id = customers.customer_id
join customer_purchase_history h on orders.customer_id = h.customer_id
left outer join payments on orders.order_id = payments.order_id
where payments.payment_status != 'fail'
)
SELECT * FROM final