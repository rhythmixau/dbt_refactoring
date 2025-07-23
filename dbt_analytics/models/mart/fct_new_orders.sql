WITH orders AS (
    SELECT 
        order_id,
        customer_id,
        ordered_at,
        store_id,
        subtotal,
        tax_paid,
        order_total
     FROM {{ ref('stg_new_orders') }}
),

order_items AS (
    SELECT 
        order_item_id,
        order_id,
        sku
    FROM
        {{ ref('stg_order_items') }}
),

products AS (
    SELECT
        product_sku,
        product_type,
        product_price
    FROM {{ ref('stg_products') }}
),

product_costs AS (
    SELECT 
        supply_sku AS product_sku,
        ROUND(SUM(supply_cost), 2) AS product_cost
    FROM {{ ref('stg_supplies') }}
    GROUP BY supply_sku
),

orders_with_types AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.ordered_at,
        o.store_id,
        o.subtotal,
        o.tax_paid,
        o.order_total,
        CASE 
            WHEN p.product_type = 'beverage' THEN True 
            ELSE False
        END AS is_drink_order,
        c.product_cost AS order_cost
    FROM orders o
    JOIN order_items i ON i.order_id = o.order_id
    JOIN products p ON p.product_sku = i.sku
    JOIN product_costs c ON c.product_sku = p.product_sku
)

SELECT
    *
FROM orders_with_types