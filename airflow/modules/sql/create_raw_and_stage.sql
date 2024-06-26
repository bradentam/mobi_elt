CREATE TABLE IF NOT EXISTS raw_data (
    trip_id VARCHAR PRIMARY KEY,
    departure_ts TIMESTAMP,
    return_ts TIMESTAMP,
    bike_id VARCHAR,
    electric_bike VARCHAR,
    departure_station VARCHAR,
    return_station VARCHAR,
    membership_type VARCHAR,
    covered_distance INTEGER,
    duration_sec INTEGER,
    departure_temperature INTEGER,
    return_temperature INTEGER,
    stopover_duration_sec INTEGER,
    number_of_stopovers INTEGER
);

CREATE TABLE IF NOT EXISTS staging_data (
    departure_ts TIMESTAMP,
    return_ts TIMESTAMP,
    bike_id VARCHAR,
    electric_bike VARCHAR,
    departure_station VARCHAR,
    return_station VARCHAR,
    membership_type VARCHAR,
    covered_distance FLOAT,
    duration_sec FLOAT,
    departure_temperature INTEGER,
    return_temperature INTEGER,
    stopover_duration_sec INTEGER,
    number_of_stopovers INTEGER
);