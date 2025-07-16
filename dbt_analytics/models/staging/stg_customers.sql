SELECT
id AS customer_id,
first_name,
last_name
FROM 
{{ source('order_gcp_source', 'customers') }}