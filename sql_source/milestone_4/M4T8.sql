-- Which German store type is selling the most?
SELECT 
    SUM(ot.product_quantity * p.product_price) AS total_sales,
    sd.store_type,
    sd.country_code
FROM 
    orders_table ot
JOIN 
    dim_products p ON ot.product_code = p.product_code
JOIN 
    dim_store_details sd ON ot.store_code = sd.store_code
WHERE 
    sd.country_code = 'DE'
GROUP BY 
    sd.store_type, sd.country_code
ORDER BY 
    total_sales;