{{
  config(
    unique_key='id' 
  )
}}

WITH distinct_memberships AS (
    SELECT DISTINCT membership_type
    FROM {{ ref('stg_bike_trips') }}
    WHERE membership_type != ''
)
SELECT
    row_number() OVER () AS id,
    membership_type
FROM
    distinct_memberships