WITH source AS (SELECT * FROM raw.category_translations)
SELECT
    product_category_name AS category_portuguese,
    product_category_name_english AS category_english
FROM source
WHERE product_category_name IS NOT NULL

