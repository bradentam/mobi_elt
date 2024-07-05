{{
  config(
    unique_key='trip_id' 
  )
}}

with staged_data as (
    select * from {{ ref('stg_dim_trip_details') }}
)

select * from staged_data