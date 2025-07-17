SELECT 
    id AS payment_id,
    orderid AS order_id,
    paymentmethod AS payment_method,
    status AS payment_status,
    {{ cents_to_dollars('amount') }} AS payment_amount,
    created AS payment_date
FROM 
    {{ source('stripe_gcp_source', 'payments') }}
WHERE status != 'fail' AND amount > 0