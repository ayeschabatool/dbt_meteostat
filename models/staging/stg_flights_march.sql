{{ config(materialized='view') }}
SELECT *
FROM {{source('flights_data', 'flights')}}
WHERE flight_date BETWEEN '2024-03-01' AND '2024-03-31'
order by flight_date