{{
  config(
    unique_key='id' 
  )
}}

with staged_data as (
    select * from {{ ref('stg_dim_memberships') }}
)

select * from staged_data