-- Updating the dim_card_details table

-- 1. Determining varchar length
SELECT MAX(LENGTH(card_number)) FROM dim_card_details; -- 19
SELECT MAX(LENGTH(expiry_date)) FROM dim_card_details; -- 5

-- 2. changing data type of columns
BEGIN;
alter table dim_card_details
alter column card_number type varchar(19),
alter column expiry_date type varchar(5),
alter column date_payment_confirmed type date;
COMMIT;

select * from dim_card_details order by 1 limit 20;