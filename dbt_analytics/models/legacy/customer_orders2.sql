WITH paid_orders as (
    select Orders.order_id,
        Orders.customer_id,
        Orders.ORDER_DATE AS order_placed_at,
            Orders.order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        C.FIRST_NAME    as customer_first_name,
            C.LAST_NAME as customer_last_name
    FROM {{ ref('stg_orders') }} as Orders
    left join (
        select 
            order_id, 
            max(payment_date) as payment_finalized_date, 
            sum(payment_amount) / 100.0 as total_amount_paid
        from {{ ref('stg_payments') }}
            where payment_status <> 'fail'
            group by 1) p ON orders.order_id = p.order_id
        left join {{ ref("stg_customers") }} C on Orders.customer_id = C.customer_id ),
customer_orders 
    as (
        select C.customer_id
            , min(ORDER_DATE) as first_order_date
            , max(ORDER_DATE) as most_recent_order_date
            , count(ORDERS.order_id) AS number_of_orders
    from {{ ref('stg_customers') }} C 
    left join {{ ref('stg_orders') }} as Orders
    on Orders.customer_id = C.customer_id 
    group by 1)
select
    p.*,
    ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
    CASE WHEN c.first_order_date = p.order_placed_at
    THEN 'new'
    ELSE 'return' END as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
    FROM paid_orders p
    left join customer_orders as c USING (customer_id)
    LEFT OUTER JOIN 
    (
            select
                p.order_id,
                sum(t2.total_amount_paid) as clv_bad
            from paid_orders p
            left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
            group by 1
            order by p.order_id
    ) x on x.order_id = p.order_id
    ORDER BY order_id