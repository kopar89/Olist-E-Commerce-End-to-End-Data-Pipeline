WITH items AS (SELECT * FROM {{ ref('stg_order_items') }}),
     products AS (SELECT * FROM {{ ref('stg_products') }}),
     translations AS (SELECT * FROM {{ ref('stg_category_translations') }})

SELECT
    COALESCE(t.category_english, p.category) AS category,
    COUNT(DISTINCT i.order_id) AS total_orders,
    ROUND(SUM(i.price)::numeric, 2) AS total_revenue,
    ROUND(AVG(i.price)::numeric, 2) AS avg_price
FROM items i
JOIN products p ON i.product_id = p.product_id
LEFT JOIN translations t ON p.category = t.category_portuguese
GROUP BY 1
ORDER BY total_revenue DESC
LIMIT 20