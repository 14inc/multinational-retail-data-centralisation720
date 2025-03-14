-- Update the dim_date_times table

-- 1. Determing varchar length
SELECT MAX(LENGTH(month)) FROM dim_date_times;
SELECT MAX(LENGTH(year)) FROM dim_date_times;
SELECT MAX(LENGTH(day)) FROM dim_date_times;
SELECT MAX(LENGTH(time_period)) FROM dim_date_times; 

-- 2. Changing data type of columns
BEGIN;
alter table dim_date_times
alter column month type varchar(2),
alter column year type varchar(4),
alter column day type varchar(2),
alter column time_period type varchar(10),
alter column date_uuid type uuid using date_uuid::uuid;
COMMIT;

select * from dim_date_times limit 10;