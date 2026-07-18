WITH source AS (SELECT * FROM raw.order_payments)
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM source
WHERE order_id IS NOT NULL
AND payment_value > 0
