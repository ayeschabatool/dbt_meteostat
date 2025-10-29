WITH daily_raw AS (
					SELECT airport_code
							,station_id
							,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
					FROM  {{ source('weather_data', 'weather_daily_raw') }}		
),
daily_flattened AS (
					SELECT airport_code
							,station_id
							,(json_data ->> 'date')::DATE AS date
							,(json_data ->> 'tavg')::NUMERIC AS avg_temp_c
							,(json_data ->> 'tmin')::NUMERIC AS min_temp_c
						FROM daily_raw
)
SELECT * FROM daily_flattened