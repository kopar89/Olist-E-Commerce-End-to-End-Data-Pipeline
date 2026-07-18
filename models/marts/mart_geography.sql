WITH orders AS (SELECT * FROM {{ ref('stg_orders') }}),
     customers AS (SELECT * FROM {{ ref('stg_customers') }}),
     items AS (SELECT * FROM {{ ref('stg_order_items') }})

SELECT
    c.state,
    c.city,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(i.price)::numeric, 2) AS total_revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN items i ON o.order_id = i.order_id
WHERE o.order_status NOT IN ('cancelled', 'unavailable')
GROUP BY 1, 2
ORDER BY total_orders DESC