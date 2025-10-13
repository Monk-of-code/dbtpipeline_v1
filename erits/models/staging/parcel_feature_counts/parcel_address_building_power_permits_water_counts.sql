{{ config(alias=named_model('parcel_address_building_power_permits_water_counts')) }}

{{ incremental_config(
    schema='Silver',
    strategy='merge',
    unique_key='uuid',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_a_b_pm_p_w_counts', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('parcel_a_b_pm_p_w_counts', suffix='uuid') }} ON {{ this }} (uuid)",
        "ANALYZE {{ this }}"
    ]
) }}

-- Base cadastre
with cadastre as (
    {{ incremental_filter(ref('cadastre_uuid'), 'uuid', strategy='merge') }}
),

-- Feature counts
power as (
    {{ incremental_filter(ref('parcel_powermeters_count'), 'uuid', strategy='merge') }}
),
water as (
    {{ incremental_filter(ref('parcel_watermeters_count'), 'uuid', strategy='merge') }}
),
building as (
    {{ incremental_filter(ref('parcel_building_count'), 'uuid', strategy='merge') }}
),
permit as (
    {{ incremental_filter(ref('parcel_permit_count'), 'uuid', strategy='merge') }}
),
address as (
    {{ incremental_filter(ref('parcel_address_count'), 'uuid', strategy='merge') }}
),

-- Raw addresses for property names
raw_addresses as (
    {{ incremental_filter(ref('addresses'), 'id', strategy='merge') }}
),

-- Join cadastre with counts
counts_joined as (
    select
        cadastre.*,
        coalesce(power.power_meter_count, 0)     as power_count,
        coalesce(water.watermeter_count, 0)     as water_count,
        coalesce(building.building_count, 0) as building_count,
        coalesce(permit.permit_count, 0)   as permit_count,
        coalesce(address.address_count, 0) as address_count
    from cadastre
    left join power    on cadastre.uuid = power.uuid
    left join water    on cadastre.uuid = water.uuid
    left join building on cadastre.uuid = building.uuid
    left join permit   on cadastre.uuid = permit.uuid
    left join address  on cadastre.uuid = address.uuid
),

-- Fuse counts into a binary indicator
fusion as (
    select
        *,
        (case when power_count   > 0 then 1 else 0 end) +
        (case when water_count   > 0 then 1 else 0 end) +
        (case when building_count > 0 then 1 else 0 end) +
        (case when permit_count  > 0 then 1 else 0 end) +
        (case when address_count > 0 then 1 else 0 end) as fusion
    from counts_joined
),

-- Link cadastre to raw addresses for propertyna
property_joined as (
    select
        c.uuid,
        c.geom,
        a.propertyna
    from cadastre c
    left join raw_addresses a
      on ST_Intersects(c.geom, a.geom)
    where a.propertyna is not null
),

-- Classify property names
classified as (
    select
        uuid,
        propertyna,
        case
            when levenshtein(lower(propertyna), 'commercial') <= 4 then 'Commercial'
            when levenshtein(lower(propertyna), 'estate') <= 4 then 'Estate'
            when levenshtein(lower(propertyna), 'mixed use') <= 4 then 'Mixed Use'
            when levenshtein(lower(propertyna), 'residential') <= 4 then 'Residential'
            else initcap(propertyna)
        end as clean_propertyna
    from property_joined
),

-- Aggregate property names per parcel
property_names as (
    select
        uuid,
        string_agg(distinct clean_propertyna, ', ' order by clean_propertyna) as property_names
    from classified
    group by uuid
),

-- Deduplicate cadastre
deduped_cadastre as (
    select distinct on (uuid) *
    from cadastre
),

-- Final merge
final as (
    select
        f.*,
        pn.property_names
    from fusion f
    left join property_names pn using (uuid)
)

select *
from final
