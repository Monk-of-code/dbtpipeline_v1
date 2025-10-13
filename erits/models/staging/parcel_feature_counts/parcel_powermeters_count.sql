{{ config(
    alias=named_model('parcel_powermeters_count'),
    materialized = 'incremental'
) }}

{{ incremental_config(
    schema='Silver',
    strategy='merge',
    unique_key='uuid',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_powermeters_count', suffix='geom') }} ON {{ this }} USING GIST (power_meter_geoms)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_powermeters_count', suffix='uuid') }} ON {{ this }} (uuid)",
        "ANALYZE {{ this }}"
    ]
) }}

with cadastre as (
    {{ incremental_filter(ref('cadastre_uuid'), 'uuid', strategy='merge' )}}
),

power_meters as (
   {{ incremental_filter(ref('power_customers'), 'fid', strategy='merge' )}}
),

-- intersections between parcels and power meters
intersections as (
    select
        c.uuid,
        c.property_ref,
        p.fid as power_meter_fid,
        p.geom as power_meter_geom
    from cadastre c
    join power_meters p
      on p.geom && c.geom               -- bbox filter
     and st_intersects(p.geom, c.geom)  -- precise check
),

-- aggregate counts + geometries per parcel
aggregated as (
    select
        i.uuid,
        i.property_ref,
        count(distinct i.power_meter_fid) as power_meter_count,
        st_union(i.power_meter_geom) as power_meter_geoms
    from intersections i
    group by i.uuid, i.property_ref
)

-- final output: all parcels (0 if no power meters)
select
    c.uuid,
    coalesce(a.power_meter_count, 0) as power_meter_count,
    a.power_meter_geoms
from cadastre c
left join aggregated a using (uuid) 
