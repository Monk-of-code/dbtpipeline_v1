{{ config(
    alias=named_model('parcel_building_count'),
    materialized = 'incremental'
) }}

{{ incremental_config(
    schema='Silver',
    strategy='merge',
    unique_key='uuid',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_building_count', suffix='geom') }} ON {{ this }} USING GIST (building_geoms)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_building_count', suffix='uuid') }} ON {{ this }} (uuid)",
        "ANALYZE {{ this }}"
    ]
) }}

with cadastre as (
    {{ incremental_filter(ref('cadastre_uuid'), 'uuid', strategy='merge' )}}
),

buildings as (
   {{ incremental_filter(ref('buildings'), 'id', strategy='merge' )}}
),

-- intersections between parcels and buildings
intersections as (
    select
        c.uuid,
        c.property_ref,
        b.id as building_id,
        b.geom as building_geom
    from cadastre c
    join buildings b
      on b.geom && c.geom               -- bbox filter
     and st_intersects(b.geom, c.geom)  -- precise check
),

-- aggregate counts + geometries per parcel
aggregated as (
    select
        i.uuid,
        i.property_ref,
        count(distinct i.building_id) as building_count,
        st_union(i.building_geom) as building_geoms
    from intersections i
    group by i.uuid, i.property_ref
)

-- final output: all parcels (0 if no buildings)
select
    c.uuid,
    coalesce(a.building_count, 0) as building_count,
    a.building_geoms
from cadastre c
left join aggregated a using (uuid)
