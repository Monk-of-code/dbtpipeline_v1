{{ config(
    alias=named_model('parcel_permit_count'),
    materialized = 'incremental'
) }}

{{ incremental_config(
    schema='Silver',
    strategy='merge',
    unique_key='uuid',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_permit_count', suffix='geom') }} ON {{ this }} USING GIST (permit_geoms)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_permit_count', suffix='uuid') }} ON {{ this }} (uuid)",
        "ANALYZE {{ this }}"
    ]
) }}

with cadastre as (
    {{ incremental_filter(ref('cadastre_uuid'), 'uuid', strategy='merge' )}}
),

permit as (
   {{ incremental_filter(ref('building_permits'), 'fid', strategy='merge' )}}
),

-- intersections between parcels and permits
intersections as (
    select
        c.uuid,
        c.property_ref,
        p.fid as permit_fid,
        p.geom as permit_geom
    from cadastre c
    join permit p
      on p.geom && c.geom               -- bbox filter
     and st_intersects(p.geom, c.geom)  -- precise check
),

-- aggregate counts + geometries per parcel
aggregated as (
    select
        i.uuid,
        i.property_ref,
        count(distinct i.permit_fid) as permit_count,
        st_union(i.permit_geom) as permit_geoms
    from intersections i
    group by i.uuid, i.property_ref
)

-- final output: all parcels (0 if no permits)
select
    c.uuid,
    coalesce(a.permit_count, 0) as permit_count,
    a.permit_geoms
from cadastre c
left join aggregated a using (uuid)
