WITH Orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
), 
Customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
agg_payments AS (
    select 
        order_id, 
        max(payment_date) as payment_finalized_date, 
        sum(payment_amount) as total_amount_paid
    from {{ ref('stg_payments') }}
        where payment_status <> 'fail'
        group by 1
)
select Orders.order_id,
    Orders.customer_id,
    Orders.ORDER_DATE AS order_placed_at,
    Orders.order_status,
    p.total_amount_paid,
    p.payment_finalized_date,
    C.FIRST_NAME    as customer_first_name,
    C.LAST_NAME as customer_last_name
FROM Orders
left join agg_payments p ON orders.order_id = p.order_id
left join Customers C on Orders.customer_id = C.customer_id 
