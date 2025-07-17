WITH orders AS (
    select 
        *
      from {{ ref('stg_orders') }}
      where order_status NOT IN ('pending') 
),
    customers AS (
        select 
        first_name || ' ' || last_name as name, 
        * 
      from {{ ref('stg_customers') }}
    ),
    payments AS (
        SELECT * FROM {{ ref('stg_payments') }} c
        WHERE payment_status != 'fail'
    ), final AS (
    select 
        c.customer_id as customer_id,
        c.name as full_name,
        c.last_name as surname,
        c.first_name as givenname,
        min(o.order_date) as first_order_date,
        min(
        o.valid_order_date) as first_non_returned_order_date,
        max(
        o.valid_order_date) as most_recent_non_returned_order_date,
        COALESCE(max(o.user_order_seq),0) as order_count,
        COALESCE(count(
            case 
                when o.order_status != 'returned' then 1 
            end),0) as non_returned_order_count,
        sum(
            case 
                when o.valid_order_date IS NOT NULL then ROUND(p.payment_amount/100.0,2) 
                else 0 
            end) as total_lifetime_value,
        sum(
            case 
                when o.valid_order_date IS NOT NULL then ROUND(p.payment_amount/100.0,2) 
                else 0 
            end)/NULLIF(count(
                case 
                    when o.valid_order_date IS NOT NULL then 1 
                end),
                    0) 
                as avg_non_returned_order_value,
        array_agg(distinct o.order_id) as order_ids
    from orders o
    join customers c
    on o.customer_id = c.customer_id
    left outer join payments p
    on o.order_id = p.order_id
    group by c.customer_id, c.name, c.last_name, c.first_name
    ) 
    select * from final
