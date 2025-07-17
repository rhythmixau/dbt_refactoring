select 
    C.customer_id,
    min(ORDER_DATE) as first_order_date,
    max(ORDER_DATE) as most_recent_order_date,
    count(ORDERS.order_id) AS number_of_orders
from {{ ref('stg_customers') }} C 
left join {{ ref('stg_orders') }} Orders on Orders.customer_id = C.customer_id 
group by 1