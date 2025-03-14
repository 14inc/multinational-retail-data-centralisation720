-- Create the primary keys in the dimension tables

-- To create primary keys and update the respective columns in different tables, you need to follow these steps:
-- 1. Ensure that the columns you want to use as primary keys have unique values and do not contain NULL values.
SELECT product_code, COUNT(*)
FROM dim_products
GROUP BY product_code
HAVING COUNT(*) > 1;

SELECT *
FROM dim_products 
WHERE product_code IS NULL;

-- 2. Alter the table to add the primary key constraint. 
BEGIN;
ALTER TABLE dim_products
ADD CONSTRAINT dim_products_pkey PRIMARY KEY (product_code);
COMMIT;

-- Repeat steps 1 and 2 for the other dim* tables