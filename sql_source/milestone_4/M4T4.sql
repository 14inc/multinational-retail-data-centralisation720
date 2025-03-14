-- How many sales are coming from online?
SELECT
    COUNT(o.product_code) AS numbers_of_sales,
    SUM(o.product_quantity) AS product_quantity_count,
    CASE
        WHEN s.store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    orders_table o
JOIN
    dim_store_details s ON o.store_code = s.store_code
GROUP BY
    location
ORDER BY
    location DESC;
