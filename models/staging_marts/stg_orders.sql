WITH source AS (
    SELECT * FROM raw.orders
)

SELECT
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchased_at,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_at,
    CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_at
FROM source
WHERE order_id IS NOT NULL
