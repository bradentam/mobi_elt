{{
  config(
    unique_key='trip_id' 
  )
}}

with station_mapping as (
    select * from {{ ref('stg_dim_stations') }}
),
membership_mapping as (
    select * from {{ ref('stg_dim_memberships') }}
)
select
    trip_id,
    bike_id,
    departure_ts,
    return_ts,
    s1.id as departure_station_id,
    s2.id as return_station_id,
    m.id as membership_id,
    covered_distance,
    duration_sec
from {{ ref('stg_bike_trips') }} t
left join station_mapping s1 
    on t.departure_station = s1.station_name
left join station_mapping s2 
    on t.return_station = s2.station_name
left join membership_mapping m 
    on t.membership_type = m.membership_type
