{{ config( alias = named_model('cadastre_landrates_tradecount_property'))}}

{{ incremental_config(
    schema = 'Silver',
    unique_key = 'uuid',
    strategy = 'merge',
    post_hook = [
        "CREATE INDEX IF NOT EXISTS {{ named_model('cadastre_landrates_tradecount_property', suffix='uuid') }} ON {{ this }} (uuid)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('cadastre_landrates_tradecount_property', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

-- ===================================================
-- 1. Cadastre base
-- ===================================================
with cadastre as (
    {{ incremental_filter(ref('parcel_address_building_power_permits_water_counts'), uuid, strategy='merge') }}
),

-- ===================================================
-- 2. Land Rates count
-- ===================================================
land_rates as (
    select uuid_derived as uuid
    from ( {{ incremental_filter(source('bronze_source', 'bd_land_rate_data'), uuid_derived, strategy='merge') }} ) as t
),
rate_counts as (
    select uuid, count(*) as rates_count
    from land_rates
    group by uuid
),
cadastre_with_rates as (
    select
        c.*,
        coalesce(r.rates_count, 0) as rates_count
    from cadastre c
    left join rate_counts r on c.property_ref = r.uuid
),

-- ===================================================
-- 3. Trade Licenses count
-- ===================================================
licenses as (
    select uuid
    from ( {{ incremental_filter(source('bronze_source', 'trade_license'), uuid, strategy='merge') }} ) as t
),
license_counts as (
    select uuid, count(*) as trade_count
    from licenses
    group by uuid
),
cadastre_with_trades as (
    select
        c.*,
        coalesce(t.trade_count, 0) as trade_count
    from cadastre_with_rates c
    left join license_counts t on c.property_ref = t.uuid
),

-- ===================================================
-- 4. Potential Interests (detailed metadata)
-- ===================================================
land_data as (
    select *, uuid_derived as uuid
    from ( {{ incremental_filter(source('bronze_source', 'bd_land_rate_data'), uuid_derived, strategy='merge') }} ) as t
),
interests as (
    select
        uuid,
        "KRA PIN"        as BL_KRAPIN,
        "Customer ID"    as BL_CustomerID,
        "Customer No"    as BL_CustomerNo,
        "Customer No1"   as BL_CustomerNo1,
        "Phone Number"   as BL_PhoneNumber,
        "Property Value" as BL_PropertyValue,
        "LRNumber"       as BL_LRNumber,
        "LR Amount"      as BL_LRAmount,
        "LR Invoice No"  as BL_LRInvoiceNo
    from land_data
    where "KRA PIN" is not null
      and "Customer ID" is not null
      and "Customer No" is not null
),

-- ===================================================
-- 5. Merge everything into enriched cadastre
-- ===================================================
final as (
    select distinct on (c.property_ref)
        c.*,
        i.BL_KRAPIN,
        i.BL_CustomerID,
        i.BL_CustomerNo,
        i.BL_CustomerNo1,
        i.BL_PhoneNumber,
        i.BL_PropertyValue,
        i.BL_LRNumber,
        i.BL_LRAmount,
        i.BL_LRInvoiceNo
    from cadastre_with_trades c
    left join interests i on c.property_ref = i.uuid
)

-- ===================================================
-- 6. Final select
-- ===================================================
select * from final
