{{
    config(
        materialized='table'
    )
}}

with fhv_trip_data as (
    select *,
       'FHV' as service_type 
    from {{ ref('stg_staging__fhv_trip_data') }}
),
dim_zones as (
    select *,
       'Zones' as service_type 
    from {{ ref('dim_zones') }}
    where pickup_locationid != 'Unknown' and dropoff_locationid != 'Unknown'
),

trips_unioned as (
    select * from fhv_trip_data
    union all
    select * from dim_zones
)
select 
     
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime,
    trips_unioned.dispatching_base_num, 
    trips_unioned.SR_Flag,
    trips_unioned.Affiliated_base_number
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.pickup_locationid = dropoff_zone.locationid