WITH departure_stations AS (
    SELECT departure_date, departure_station 
    FROM {{ ref('staging_bike_trips') }}
),

return_stations AS (
    SELECT departure_date, return_station 
    FROM {{ ref('staging_bike_trips') }}
),

all_stations AS (
    (SELECT 
        departure_date,
        LEFT(departure_station, 4) AS id,
        RIGHT(departure_station, LEN(departure_station) - 5) AS station
    FROM departure_stations)
    UNION
    (SELECT 
        departure_date,
        LEFT(return_station, 4) AS id,
        RIGHT(return_station, LEN(return_station) - 5) AS station
    FROM return_stations)
),

cleaned_stations AS (
SELECT
    departure_date,
    id,
    station,
    row_number() OVER (PARTITION BY id ORDER BY departure_date DESC) AS num
FROM all_stations
)
SELECT * FROM cleaned_stations