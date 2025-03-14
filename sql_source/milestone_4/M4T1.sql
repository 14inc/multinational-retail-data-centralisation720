-- How many stores does the business have and in which countries?
SELECT country_code AS country, COUNT("index") AS total_no_stores
FROM dim_store_details
WHERE store_type <> 'Web Portal'
GROUP BY country_code
ORDER BY total_no_stores DESC