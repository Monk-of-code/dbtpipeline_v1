{{ config(
    alias=named_model('parcel_watermeters_count'),
    materialized = 'incremental'
) }}

{{ incremental_config(
    schema='Silver',
    strategy='merge',
    unique_key='uuid',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_watermeters_count', suffix='geom') }} ON {{ this }} USING GIST (watermeter_geoms)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_watermeters_count', suffix='uuid') }} ON {{ this }} (uuid)",
        "ANALYZE {{ this }}"
    ]
) }}

with cadastre as (
    {{ incremental_filter(ref('cadastre_uuid'), 'uuid', strategy='merge' )}}
),

watermeters as (
   {{ incremental_filter(ref('water_meters'), 'fid', strategy='merge' )}}
),

-- intersections between parcels and watermeters
intersections as (
    select
        c.uuid,
        c.property_ref,
        p.fid as watermeter_fid,
        p.geom as watermeter_geom
    from cadastre c
    join watermeters p
      on p.geom && c.geom               -- bbox filter
     and st_intersects(p.geom, c.geom)  -- precise check
),

-- aggregate counts + geometries per parcel
aggregated as (
    select
        i.uuid,
        i.property_ref,
        count(distinct i.watermeter_fid) as watermeter_count,
        st_union(i.watermeter_geom) as watermeter_geoms
    from intersections i
    group by i.uuid, i.property_ref
)

-- final output: all parcels (0 if no watermeters)
select
    c.uuid,
    coalesce(a.watermeter_count, 0) as watermeter_count,
    a.watermeter_geoms
from cadastre c
left join aggregated a using (uuid)
