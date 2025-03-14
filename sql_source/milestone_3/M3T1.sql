-- Cast the columns of the orders_table to the correct data types

-- 1. Determine maximum length of the values in each column to be changed to varchar
SELECT MAX(LENGTH(card_number)) FROM orders_table;
SELECT MAX(LENGTH(store_code)) FROM orders_table;
SELECT MAX(LENGTH(product_code)) FROM orders_table;

-- 2. Change table column data types
BEGIN;
alter table orders_table
alter column date_uuid type uuid using date_uuid::uuid,
alter column user_uuid type uuid using user_uuid::uuid,
alter column card_number type varchar(19),
alter column store_code type varchar(12),
alter column product_code type varchar(11),
alter column product_quantity type smallint;
select * from orders_table limit 5;
COMMIT;

