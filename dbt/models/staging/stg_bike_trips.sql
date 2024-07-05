select
    trip_id,
    departure_ts,
    cast(departure_ts as date) as departure_date,
    date_part(year, departure_ts) as departure_year,
    date_part(month, departure_ts) as departure_month,
    date_part(day, departure_ts) as departure_day,
    date_part(dayofweek, departure_ts) as departure_day_of_week,
    date_part(hour, departure_ts) as departure_hour,
    case
        when date_part(dayofweek, departure_ts) in (0, 6) then true else false
    end as departure_weekend,
    return_ts,
    cast(return_ts as date) as return_date,
    date_part(year, return_ts) as return_year,
    date_part(month, return_ts) as return_month,
    date_part(day, return_ts) as return_day,
    date_part(dayofweek, return_ts) as return_day_of_week,
    date_part(hour, return_ts) as return_hour,
    case
        when date_part(dayofweek, return_ts) in (0, 6) then true else false
    end as return_weekend,
    bike_id,
    electric_bike,
    departure_station,
    return_station,
    membership_type,
    covered_distance,
    duration_sec,
    covered_distance / duration_sec as avg_speed,
    departure_temperature,
    return_temperature,
    stopover_duration_sec,
    number_of_stopovers
from {{ source("raw", "raw_data") }}

    -- {% if is_incremental() %}

    -- -- Only process new records that have been added since the last run
    -- where departure_ts >= '2024-04-01' --and departure_ts <= '2024-05-01' --change for macro variable or (select max(departure_ts) from mart tables)

    -- {% endif %}
    where (departure_ts >= '2023-05-01')-- and (departure_ts <= '2024-05-01')
