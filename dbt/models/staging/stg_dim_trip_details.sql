{{
  config(
    unique_key='trip_id'
  )
}}

select
    trip_id,
    departure_date,
    departure_year,
    departure_month,
    departure_day,
    departure_day_of_week,
    departure_hour,
    departure_weekend,
    return_date,
    return_year,
    return_month,
    return_day,
    return_day_of_week,
    return_hour,
    return_weekend,
    departure_temperature,
    return_temperature,
    stopover_duration_sec,
    number_of_stopovers,
    avg_speed
    
from
    {{ ref('stg_bike_trips') }}