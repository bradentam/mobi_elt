WITH departure_stations AS (
    SELECT departure_station 
    FROM {{ ref('staging_bike_trips') }}
),

return_stations AS (
    SELECT return_station 
    FROM {{ ref('staging_bike_trips') }}
),

cleaned_stations AS (
    (SELECT 
        LEFT(departure_station, 4) AS id,
        RIGHT(departure_station, LEN(departure_station) - 5) AS station
    FROM departure_stations)
    UNION
    (SELECT 
        LEFT(return_station, 4) AS id,
        RIGHT(return_station, LEN(return_station) - 5) AS station
    FROM return_stations)

)

SELECT * FROM cleaned_stations