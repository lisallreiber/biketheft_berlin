{{ config(materialized='table') }}

with theft_reports_daily as (
    select *, 
        'daily' as source_type
    from {{ ref('stg_theft_reports_daily') }}
), 

theft_reports_archived as (
    select *, 
        'archive' as source_type
    from {{ ref('stg_theft_reports_archived') }}
), 

reports_unioned as (
    select * from theft_reports_archived
    union all
    select * from theft_reports_daily
), 

dim_lor_geo as (
    select * from {{ ref('stg_berlin_lor_geo') }}
)

select 
    reports_unioned.record_id, 
    reports_unioned.damage_euro,
    reports_unioned.bike_type, 
    reports_unioned.crime_period_start_datetime, 
    -- reports_unioned.crime_period_start_date, 
    -- reports_unioned.crime_period_end_date,
    reports_unioned.crime_period_end_datetime,
    reports_unioned.source_type,
    dim_berlin_lor.bezirk_id as crime_location_bezirk_id,
    dim_berlin_lor.bezirk_name as crime_location_bezirk_name,
    dim_berlin_lor.pgr_id as crime_location_pgr_id,
    dim_berlin_lor.pgr_name as crime_location_pgr_name,
    dim_berlin_lor.plr_id as crime_location_plr_id,
    dim_berlin_lor.plr_name as crime_location_plr_name,
    dim_berlin_lor.plr_geometry as berlin_plr_geometry,
    dim_berlin_lor.stand as berlin_plr_geometry_reference_date

from reports_unioned
left join dim_lor_geo as dim_berlin_lor 
on reports_unioned.plr_id = dim_berlin_lor.plr_id