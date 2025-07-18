{{
  config(
    materialized = 'incremental',
    unique_key = 'order_id',
    incremental_strategy = 'merge',
    on_schema_change = 'fail'
  )
}}

with orders as  (
    select * from {{ ref ('stg_orders' )}}
),

payments as (
    select * from {{ ref ('stg_payments') }}
),

order_payments as (
    select
        order_id,
        sum (case when payment_status = 'success' then payment_amount end) as amount

    from payments
    group by 1
),

 final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce (order_payments.amount, 0) as amount

    from orders
    left join order_payments using (order_id)
)

select * from final
{% if is_incremental() %}
    where order_date >= (select max(order_date) from {{this}})
{% endif %}