-- Cast the columns of the dim_users to the correct data types

-- 1. Determine maximum length of the values in each column to be changed to varchar
-- first_name and last_name have a length of 255
SELECT MAX(LENGTH(country_code)) FROM dim_users;

-- 2. Change table column data types
BEGIN;
alter table dim_users
alter column first_name type varchar(255),
alter column last_name type varchar(255),
alter column date_of_birth type date,
alter column country_code type varchar(3),
alter column user_uuid type uuid using user_uuid::uuid,
alter column join_date type date;
select * from dim_users limit 5;
COMMIT;