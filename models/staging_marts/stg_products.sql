WITH source AS (
    SELECT * FROM raw.products
)
SELECT
    product_id,
    COALESCE(product_category_name, 'unknown') AS category,
    product_weight_g AS weight_g,
    product_length_cm AS length_cm,
    product_height_cm AS height_cm,
    product_width_cm AS width_cm
FROM source
WHERE product_id IS NOT NULL
AND product_category_name <> 'unknown'
AND product_weight_g > 0
AND product_length_cm > 0
AND product_height_cm > 0
AND product_width_cm > 0