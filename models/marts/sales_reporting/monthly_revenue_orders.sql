SELECT
    EXTRACT(month from order_date) as month,
    EXTRACT(year from order_date) as year,
    COUNT(order_id) as total_orders,
    SUM(total_price) as total_revenue
FROM {{ ref('stg_sales') }}
GROUP BY EXTRACT(month from order_date), EXTRACT(year from order_date)
ORDER BY EXTRACT(month from order_date), EXTRACT(year from order_date)