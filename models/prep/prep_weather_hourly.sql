WITH hourly_data AS (
    SELECT * 
    FROM {{ref('stg_weather_hourly')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
		, timestamp::time AS time                           -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
        , TO_CHAR(timestamp, 'FMmonth') AS weekday        -- weekday name as TEXT        
        , DATE_PART('day', timestamp) AS date_day
		, date_part('month',timestamp) AS date_month
		, date_part('year',timestamp) AS date_year
		, date_part('week',timestamp) AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE 
			WHEN time BETWEEN '00:00:00' AND '05:59:00' THEN 'night'
			WHEN time between '06:00:00' AND '18:00:00' THEN 'day'
			WHEN time BETWEEN '18:00:00' AND '23:59:00' THEN 'evening'
		END) AS day_part
    FROM add_features
)

SELECT *
FROM add_more_features