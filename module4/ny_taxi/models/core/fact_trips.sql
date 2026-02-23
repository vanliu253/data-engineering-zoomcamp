{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select 
        *,
        'Green' as service_type
    from 
        {{ref('stg_green_tripdata')}}
),

yellow_tripdata as (
    select
        *,
        'Yellow' as service_type
    from
        {{ref('stg_yellow_tripdata')}}
),

trips_unioned as (
    select
        *
    from
        yellow_tripdata 
    union all 
    select 
        * 
    from 
        green_tripdata
),

dim_zones as (
    select * from {{ref('dim_zones')}}
)

select 
    * 
from 
    trips_unioned tu
inner join
    dim_zones pickup_zone
on
    tu.pickup_location_id = pickup_zone.locationid
inner join
    dim_zones dropoff_zone
on
    tu.dropoff_location_id = dropoff_zone.locationid
