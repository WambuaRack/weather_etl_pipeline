SELECT
    city,
    weather_date,
    AVG(temperature) AS avg_temp,
    AVG(humidity) AS avg_humidity,
    AVG(wind_speed) AS avg_wind_speed
FROM {{ ref('stg_weather') }}
GROUP BY city, weather_date
