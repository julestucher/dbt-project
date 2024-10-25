SELECT 
    products.category AS category,
    SUM(sales.quantity) AS total_sales,
    SUM(sales.total_price) AS total_revenue,
    avg_prices.avg_price
FROM {{ ref('stg_sales') }} as sales
LEFT JOIN {{ ref('stg_products') }} as products ON sales.product_id = products.product_id
LEFT JOIN (
    -- pre-calculate avg price so prices aren't duplicated in merge
    SELECT 
        category,
        AVG(price) as avg_price
    FROM  {{ ref('stg_products')}}
    GROUP BY category
    ) avg_prices ON avg_prices.category = products.category
GROUP BY products.category, avg_prices.avg_price