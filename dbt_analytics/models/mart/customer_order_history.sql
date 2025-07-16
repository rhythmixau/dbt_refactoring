WITH orders AS (
    select 
        row_number() over (partition by user_id order by order_date, order_id) as user_order_seq,
        *
      from {{ ref('stg_orders') }}
),
    customers AS (
        select 
        first_name || ' ' || last_name as name, 
        * 
      from {{ ref('stg_customers') }}
    ),
    payments AS (
        SELECT * FROM {{ ref('stg_payments') }} c
    ), final AS (
    select 
        b.customer_id as customer_id,
        b.name as full_name,
        b.last_name as surname,
        b.first_name as givenname,
        min(order_date) as first_order_date,
        min(
        case 
            when a.order_status NOT IN ('returned','return_pending') then order_date 
        end) as first_non_returned_order_date,
        max(
        case 
            when a.order_status NOT IN ('returned','return_pending') then order_date 
        end) as most_recent_non_returned_order_date,
        COALESCE(max(user_order_seq),0) as order_count,
        COALESCE(count(
            case 
                when a.order_status != 'returned' then 1 
            end),0) as non_returned_order_count,
        sum(
            case 
                when a.order_status NOT IN ('returned','return_pending') then ROUND(c.payment_amount/100.0,2) 
                else 0 
            end) as total_lifetime_value,
        sum(
            case 
                when a.order_status NOT IN ('returned','return_pending') then ROUND(c.payment_amount/100.0,2) 
                else 0 
            end)/NULLIF(count(
                case 
                    when a.order_status NOT IN ('returned','return_pending') then 1 
                end),
                    0) 
                as avg_non_returned_order_value,
        array_agg(distinct a.order_id) as order_ids
    from orders a
    join customers b
    on a.user_id = b.customer_id
    left outer join payments c
    on a.order_id = c.order_id
    where a.order_status NOT IN ('pending') and c.payment_status != 'fail'
    group by b.customer_id, b.name, b.last_name, b.first_name
    ) 
    select * from final
