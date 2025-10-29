{{config(materialized='view')}}
select * from {{source('flights_data','flights')}}
where flight_date::date between '2024-01-01' and '2024-01-31'
order by flight_date