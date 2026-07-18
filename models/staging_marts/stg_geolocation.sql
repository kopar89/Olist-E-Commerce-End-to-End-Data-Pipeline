WITH source AS (SELECT * FROM raw.geolocation)
SELECT
    geolocation_zip_code_prefix AS zip_code,
    geolocation_lat AS latitude,
    geolocation_lng AS longitude,
    LOWER(TRIM(geolocation_city)) AS city,
    UPPER(TRIM(geolocation_state)) AS state
FROM source
WHERE geolocation_zip_code_prefix IS NOT NULL
AND geolocation_lat BETWEEN -90 AND 90
AND geolocation_lng BETWEEN -180 AND 180
