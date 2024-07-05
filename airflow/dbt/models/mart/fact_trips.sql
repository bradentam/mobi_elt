{{
  config(
    unique_key='trip_id' 
  )
}}

with staged_data as (
    select * from {{ ref('stg_fact_trips') }}
)

select * from staged_data