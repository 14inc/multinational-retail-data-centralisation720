-- What percentage of sales come through each type of store?
SELECT
    store_type,
    SUM(o.product_quantity * p.product_price) AS total_sales,
    ROUND((SUM(o.product_quantity * p.product_price) / (SELECT SUM(product_quantity * product_price) FROM orders_table o2 JOIN dim_products p2 ON o2.product_code = p2.product_code)) * 100, 2) AS "sales_made(%)"
FROM
    orders_table o
JOIN
    dim_store_details s ON o.store_code = s.store_code
JOIN
    dim_products p ON o.product_code = p.product_code
GROUP BY
    store_type
ORDER BY
    "sales_made(%)" DESC;