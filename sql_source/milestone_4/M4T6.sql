-- Which month in each year produced the highest cost of sales?
WITH monthly_sales AS (
    SELECT
        dt.year,
        dt.month,
        SUM(o.product_quantity * p.product_price) AS total_sales
    FROM
        orders_table o
    JOIN
        dim_date_times dt ON o.date_uuid = dt.date_uuid
    JOIN
        dim_products p ON o.product_code = p.product_code
    GROUP BY
        dt.year, dt.month
),
ranked_sales AS (
    SELECT
        total_sales,
        year,
        month,
        RANK() OVER (PARTITION BY year ORDER BY total_sales DESC) AS sales_rank
    FROM
        monthly_sales
)
SELECT
    total_sales,
    year,
    month
FROM
    ranked_sales
WHERE
    sales_rank = 1
ORDER BY
    total_sales DESC;