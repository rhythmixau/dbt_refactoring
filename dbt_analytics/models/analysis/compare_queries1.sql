{% set old_query %}
  select
    order_id,
    order_value_dollars,
    customer_id
  from {{ ref("customer_orders") }}
{% endset %}

{% set new_query %}
  select
    order_id,
    order_value_dollars,
    customer_id
  from {{ ref('fct_customer_orders') }}
{% endset %}

{{ 
  audit_helper.compare_and_classify_query_results(
    old_query, 
    new_query, 
    primary_key_columns=['order_id'], 
    columns=['order_id', 'order_value_dollars', 'customer_id']
  )
}}

-- 