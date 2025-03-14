-- Update the dim_store_details table: merge columns, change data types and update row.

-- 1. Determine maximum length of the values in each column to be changed to varchar
SELECT MAX(LENGTH(store_code)) FROM dim_store_details;
SELECT MAX(LENGTH(country_code)) FROM dim_store_details;

-- 2. Merge latitude and lat columns into one.
BEGIN;
-- Step 1: Update the latitude column with values from the lat column where latitude is NULL
UPDATE dim_store_details
SET latitude = lat
WHERE latitude IS NULL;
-- Step 2: Drop the lat column
ALTER TABLE dim_store_details
DROP COLUMN lat;
COMMIT;

-- 3. There is a row that represents the business's website 
-- change the location column values from N/A to NULL
BEGIN;
UPDATE dim_store_details
SET longitude = NULL,
	locality = NULL,
	address = NULL,
	latitude = NULL
WHERE longitude = 'N/A' AND latitude = 'N/A' AND store_type = 'Web Portal';
COMMIT;

-- 4. Change table column data types
BEGIN;
alter table dim_store_details
alter column longitude type numeric using longitude::numeric,
alter column locality type varchar(255),
alter column store_code type varchar(12),
alter column staff_numbers type smallint USING staff_numbers::smallint,
alter column opening_date type date,
alter column store_type type varchar(255),
ALTER COLUMN store_type DROP NOT NULL,
alter column latitude type numeric using latitude::numeric,
alter column country_code type varchar(2),
alter column continent type varchar(255);
COMMIT;