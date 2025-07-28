-- Change the default behaviour of the default behaviour
{{
    config(severity='warn')
}}

SELECT 
    order_count
FROM {{ ref('customer_order_history') }}
WHERE order_count <= 0
-- The condition should be the opposite of what you are testing
-- An error will be thrown when the result is not empty