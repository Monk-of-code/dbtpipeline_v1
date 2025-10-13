{{ config(
    alias=named_model('parcel_address_count'),
    materialized = 'incremental'
) }}

{{ incremental_config(
    schema='Silver',
    strategy='merge',
    unique_key='uuid',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_address_count', suffix='geom') }} ON {{ this }} USING GIST (address_geoms)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_address_count', suffix='uuid') }} ON {{ this }} (uuid)",
        "ANALYZE {{ this }}"
    ]
) }}

with cadastre as (
    {{ incremental_filter(ref('cadastre_uuid'), 'uuid', strategy='merge' )}}
),

address as (
   {{ incremental_filter(ref('addresses'), 'id', strategy='merge' )}}
),

-- intersections between parcels and addresses
intersections as (
    select
        c.uuid,
        c.property_ref,
        a.id as address_id,
        a.geom as address_geom
    from cadastre c
    join address a
      on a.geom && c.geom               -- bbox filter
     and st_intersects(a.geom, c.geom)  -- precise check
),

-- aggregate counts + geometries per parcel
aggregated as (
    select
        i.uuid,
        i.property_ref,
        count(distinct i.address_id) as address_count,
        st_union(i.address_geom) as address_geoms
    from intersections i
    group by i.uuid, i.property_ref
)

-- final output: all parcels (0 if no addresses)
select
    c.uuid,
    coalesce(a.address_count, 0) as address_count,
    a.address_geoms
from cadastre c
left join aggregated a using (uuid)
