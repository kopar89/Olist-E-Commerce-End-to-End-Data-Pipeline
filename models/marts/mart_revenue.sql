WITH orders AS (SELECT * FROM {{ ref('stg_orders') }}),
     items AS (SELECT * FROM {{ ref('stg_order_items') }})

SELECT
    DATE_TRUNC('month', o.order_purchased_at) AS month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(i.price)::numeric, 2) AS total_revenue,
    ROUND(AVG(i.price)::numeric, 2) AS avg_order_value
FROM orders o
JOIN items i ON o.order_id = i.order_id
WHERE o.order_status NOT IN ('cancelled', 'unavailable')
GROUP BY 1
ORDER BY 1