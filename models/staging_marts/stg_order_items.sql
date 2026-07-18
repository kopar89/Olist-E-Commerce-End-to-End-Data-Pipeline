WITH source AS (SELECT * FROM raw.order_items)
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_at,
    price,
    freight_value
FROM source
WHERE order_id IS NOT NULL
AND price > 0
