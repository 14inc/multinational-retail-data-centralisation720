-- How quickly is the company making sales?
WITH sale_times AS (
    SELECT 
        ot.date_uuid,
        dt.year,
        dt.month,
        dt.day,
        -- Create full timestamp by combining year, month, day, and time
        TO_TIMESTAMP(dt.year || '-' || dt.month || '-' || dt.day || ' ' || dt.timestamp, 'YYYY-MM-DD HH24:MI:SS') AS full_timestamp,
        
        -- Calculate next sale's timestamp using LEAD() without partitioning by year
        LEAD(
            TO_TIMESTAMP(dt.year || '-' || dt.month || '-' || dt.day || ' ' || dt.timestamp, 'YYYY-MM-DD HH24:MI:SS')
        ) OVER (ORDER BY TO_TIMESTAMP(dt.year || '-' || dt.month || '-' || dt.day || ' ' || dt.timestamp, 'YYYY-MM-DD HH24:MI:SS') DESC) AS next_full_timestamp
    FROM 
        orders_table ot
    JOIN 
        dim_date_times dt ON ot.date_uuid = dt.date_uuid
)
SELECT 
    year,
    TO_CHAR(
        INTERVAL '1 second' * AVG(
            -- Calculate the average time taken (in seconds) between sales
            EXTRACT(EPOCH FROM full_timestamp - next_full_timestamp)
        ),
        'HH24:MI:SS'  -- Format the result as HH:MM:SS
    ) AS actual_time_taken
FROM 
    sale_times
WHERE 
    next_full_timestamp IS NOT NULL  -- Ignore rows with no next sale timestamp
GROUP BY 
    year
ORDER BY 
    actual_time_taken DESC;  -- Order by average time taken in descending order
