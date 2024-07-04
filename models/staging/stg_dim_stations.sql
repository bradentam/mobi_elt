{{
  config(
    unique_key='id'
  )
}}

with departure_stations as (
    select distinct departure_station as station_name 
    from {{ ref('stg_bike_trips') }}
),
return_stations as (
    select distinct return_station as station_name 
    from {{ ref('stg_bike_trips') }}
),
all_stations as (
    select station_name from departure_stations
    union
    select station_name from return_stations
),
cleaned_stations as (
    select
        row_number() over () as id,
        station_name
    from
        all_stations)

select * from cleaned_stations

-- WITH departure_stations AS (
--     SELECT departure_date, departure_station 
--     FROM {{ ref('stg_bike_trips') }}
-- ),

-- return_stations AS (
--     SELECT departure_date, return_station 
--     FROM {{ ref('stg_bike_trips') }}
-- ),
-- all_stations AS (
--     (SELECT 
--         departure_date,
--         LEFT(departure_station, 4) AS id,
--         RIGHT(departure_station, LEN(departure_station) - 5) AS station_name
--     FROM departure_stations)
--     UNION
--     (SELECT 
--         departure_date,
--         LEFT(return_station, 4) AS id,
--         RIGHT(return_station, LEN(return_station) - 5) AS station_name
--     FROM return_stations)
-- ),

-- cleaned_stations AS (
-- SELECT
--     id,
--     station_name
-- FROM (SELECT
--         departure_date,
--         id,
--         station_name,
--         row_number() OVER (PARTITION BY id ORDER BY departure_date DESC) AS num
--     FROM all_stations)
-- WHERE num = 1
-- )
-- SELECT * FROM cleaned_stations