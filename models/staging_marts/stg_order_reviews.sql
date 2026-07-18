WITH source AS (SELECT * FROM raw.order_reviews)
SELECT
    review_id,
    order_id,
    review_score,
    CAST(review_creation_date AS TIMESTAMP) AS created_at
FROM source
WHERE review_id IS NOT NULL
AND review_score BETWEEN 1 AND 5
