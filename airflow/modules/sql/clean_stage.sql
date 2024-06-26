CREATE TABLE cleaned_staging_data AS
SELECT DISTINCT
    departure_ts,
    return_ts,
    bike_id,
    electric_bike,
    departure_station,
    return_station,
    membership_type,
    ROUND(covered_distance) AS covered_distance,
    ROUND(duration_sec) AS duration_sec,
    departure_temperature,
    return_temperature,
    stopover_duration_sec,
    number_of_stopovers
FROM
    staging_data
WHERE
    departure_ts IS NOT NULL
    AND return_ts IS NOT NULL
    AND covered_distance > 0
    AND duration_sec > 0;