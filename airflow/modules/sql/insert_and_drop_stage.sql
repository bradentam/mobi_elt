INSERT INTO raw_data 
SELECT
    md5(departure_ts || return_ts || electric_bike || departure_station || 
        return_station || membership_type || covered_distance || duration_sec || 
        departure_temperature || return_temperature || stopover_duration_sec || 
        number_of_stopovers) AS trip_id,
    departure_ts,
    return_ts,
    bike_id,
    electric_bike,
    departure_station,
    return_station,
    membership_type,
    covered_distance,
    duration_sec,
    departure_temperature,
    return_temperature,
    stopover_duration_sec,
    number_of_stopovers
FROM cleaned_staging_data
WHERE NOT EXISTS (
SELECT 1 
FROM raw_data
WHERE raw_data.trip_id = md5(cleaned_staging_data.departure_ts || cleaned_staging_data.return_ts || 
                             cleaned_staging_data.electric_bike || cleaned_staging_data.departure_station || 
                             cleaned_staging_data.return_station || cleaned_staging_data.membership_type || 
                             cleaned_staging_data.covered_distance || cleaned_staging_data.duration_sec || 
                             cleaned_staging_data.departure_temperature || cleaned_staging_data.return_temperature || 
                             cleaned_staging_data.stopover_duration_sec || cleaned_staging_data.number_of_stopovers)
)
;

DROP TABLE IF EXISTS staging_data, cleaned_staging_data;
