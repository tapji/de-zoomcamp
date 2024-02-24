{{ config(
    materialized='view'
)}}

with tripdata as 
(
  select *,
    row_number() over(partition by pickup_datetime) as rn
  from {{ source('staging','fhv_trip_data') }}
  where extract(year from pickup_datetime) = 2019 
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("PUlocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("integer")) }} as dispatching_base_num,
    SR_Flag,
    {{ dbt.safe_cast("Affiliated_base_number", api.Column.translate_type("integer")) }} as Affiliated_base_number

from tripdata
where rn = 1

{% if var('is_test_run', default=true) %}
limit 100
{% endif %}