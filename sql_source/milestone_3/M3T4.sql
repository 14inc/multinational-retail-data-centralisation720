-- Make changes to the dim_products table for the delivery team

-- 1. Remove £ from values in product_price column
BEGIN;
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

-- 2. Change datatype of product_type so this column can be used for calculation
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE DOUBLE PRECISION USING product_price::DOUBLE PRECISION;

-- 3. Add a new column called weight_class and populate it
-- Step 1: Add the new column weight_class
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(14);
-- Step 2: Update the weight_class column based on the weight range
UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    ELSE 'Truck_Required'
END;
COMMIT;