{{ config(materialized='table') }}

with trips_data as (
    select * from {{ref('fact_trips')}}
)

select
    service_zone as revenue_zone,
    {{ dbt.date_trunc('month', 'pickup_datetime') }} as revenue_month,
    service_type,
    sum(fare_amount) as revenue_monthly_fee,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,
    count(trip_id) as total_monthly_trips,
    avg(passenger_count) as avg_monthly_passenger_count,
    avg(trip_distance) as avg_monthly_trip_distance
from
    trips_data
group by
    service_zone,
    revenue_month,
    service_type
     
