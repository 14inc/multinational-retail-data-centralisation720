-- Update the dim_products table with the required data types

-- 1. Renaming column
BEGIN;
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;
select * from dim_products order by 1;
commit

-- 2. Determing varchar length
SELECT MAX(LENGTH("EAN")) FROM dim_products; -- 17
SELECT MAX(LENGTH(product_code)) FROM dim_products; -- 11

-- 3. Cleaning column
BEGIN;
UPDATE dim_products
SET still_available = CASE
    WHEN still_available = 'Still_avaliable' THEN 'TRUE'
    WHEN still_available = 'Removed' THEN 'FALSE'
END;
COMMIT;

-- 4. Changing data type of columns
BEGIN;
alter table dim_products
alter column product_price type numeric using product_price::numeric,
alter column weight type numeric using weight::numeric,
alter column "EAN" type varchar(17),
alter column product_code type varchar(11),
alter column date_added type date,
alter column "uuid" type uuid using "uuid"::uuid,
alter column still_available type bool USING still_available::boolean;
-- weight_class has been created as varchar(14) in M3T4
COMMIT;

select * from dim_products order by 1 limit 10;