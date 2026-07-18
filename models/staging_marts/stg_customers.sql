WITH source AS (SELECT * FROM raw.customers)
SELECT
    customer_id,
    customer_unique_id,
    LOWER(TRIM(customer_city)) AS city,
    UPPER(TRIM(customer_state)) AS state,
    customer_zip_code_prefix AS zip_code
FROM source
WHERE customer_id IS NOT NULL
