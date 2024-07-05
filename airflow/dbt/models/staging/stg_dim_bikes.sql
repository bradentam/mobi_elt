{{
  config(
    unique_key='id' 
  )
}}

select distinct 
    bike_id as id,
    electric_bike
from
    {{ ref('stg_bike_trips') }}
where bike_id != ''
order by bike_id