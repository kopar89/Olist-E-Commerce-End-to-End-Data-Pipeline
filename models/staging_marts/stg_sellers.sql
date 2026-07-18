WITH source AS (SELECT * FROM raw.sellers)
SELECT
    seller_id,
    seller_zip_code_prefix AS zip_code,
    LOWER(TRIM(seller_city)) AS city,
    UPPER(TRIM(seller_state)) AS state
FROM source
WHERE seller_id IS NOT NULL
