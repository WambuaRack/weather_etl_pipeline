SELECT
    city,
    temperature,
    humidity,
    wind_speed,
    ingestion_time::date AS weather_date
FROM raw.weather
