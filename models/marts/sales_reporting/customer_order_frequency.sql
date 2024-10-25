SELECT
    customer_id,
    COUNT(order_id) as total_orders,
    MIN(order_date) as first_order,
    MAX(order_date) as last_order,
    AVG({{ datediff("prev_order_date", "order_date", "day") }}) as avg_days_between_orders
FROM (
    SELECT
        customer_id,
        order_id,
        order_date,
        -- calculate date of last order
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_order_date
    FROM {{ ref('stg_sales') }}
) 
GROUP BY customer_id
ORDER BY customer_id