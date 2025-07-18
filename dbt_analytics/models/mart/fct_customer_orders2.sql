{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='append'
    )
}}
-- incremental_strategy = 'append|merge|delete+insert|upsert|insert_overwrite'
--                        insert_overwrite

-- more efficient on BigQuery
-- config(
--         materialized='incremental',
--         unique_key='order_id',
--         partition_by={"field": "order_date", "data_type": "date", "granularity": "day"}
--         incremental_strategy='insert_overwrite'
--     )

WITH paid_orders as (
    select 
        *
    FROM {{ ref('customer_paid_orders')}}
),
customer_orders as (
    select 
        *
    from {{ ref('customer_orders_agg') }}
),
customer_livetime_values AS (
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
        left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
        group by 1
        order by p.order_id
), 
final AS (
    select
        p.*,
        ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
        CASE 
            WHEN ( RANK() OVER(
                PARTITION BY c.customer_id 
                ORDER BY p.order_placed_at, p.order_id
            ) = 1 ) THEN 'new'
            ELSE 'return' 
        END as nvsr,
        clv.clv_bad as customer_lifetime_value,
        c.first_order_date as fdos,
        CURRENT_TIMESTAMP as updated_at
    FROM paid_orders p
        left join customer_orders as c USING (customer_id)
        LEFT OUTER JOIN customer_livetime_values clv on clv.order_id = p.order_id
    ORDER BY order_id
)
SELECT * FROM final
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    WHERE order_placed_at > (SELECT MAX(order_placed_at) FROM {{ this }}) 
{% endif %}
ORDER BY order_placed_at DESC