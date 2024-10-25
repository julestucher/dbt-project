SELECT
    customer_id,
    COUNT(order_id) as total_orders,
    SUM(total_price) as total_revenue
FROM {{ ref('stg_sales') }}
GROUP BY customer_id
ORDER BY SUM(total_price) desc limit 10