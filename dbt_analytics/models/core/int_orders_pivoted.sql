{%- set payment_methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card'] -%}

WITH payments AS (
    SELECT * FROM {{ ref("stg_payments") }}
),

pivoted AS (
    SELECT 
        order_id,
        {% for method in payment_methods -%}
        sum(CASE WHEN payment_method = '{{ method }}' THEN payment_amount ELSE 0 END) AS {{ method }}_amount
        {%- if not loop.last -%}
        , 
        {% endif -%}
        {% endfor %}
    FROM
        payments
    WHERE payment_status = 'success'
    GROUP BY order_id
)

SELECT * FROM pivoted