{% test assert_column_is_greater_than_zero(model, column_name) %}
SELECT 
    {{ column_name }}
FROM {{ model }}
WHERE order_count <= 0
{% endtest %}