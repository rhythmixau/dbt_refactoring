WITH supplies AS (
    SELECT 
        id,
        name, 
        cost,
        perishable, 
        sku
    FROM {{ source('order_gcp_new_source', 'raw_supplies') }}
)

SELECT 
    id AS supply_id,
    name AS supply_name,
    cost/100.0 AS supply_cost,
    perishable,
    sku AS supply_sku
FROM supplies