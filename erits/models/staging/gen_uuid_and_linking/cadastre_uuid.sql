{{ config(
    alias=named_model('cadastre_parcel_uuid'),
    materialized = 'incremental',
    pre_hook=[
        "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""
    ]
) }}


{{ incremental_config(
    unique_key='id',
    strategy='merge',
    schema='Silver',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('cadastre_uuid', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('cadastre_uuid', suffix='uuid') }} ON {{ this }} (uuid)",
        "ANALYZE {{ this }}"
    ]
) }}



with cadastre as (
    {{ incremental_filter(ref('cadastre_parcel'), 'id', strategy='merge') }}
),

property_ref as (
    select 
        *,
        COALESCE(
            NULLIF(TRIM(parcel_number), ''),
            NULLIF(TRIM(lr_number), ''),
            'uuid_' || id::text
        ) as property_ref
    from cadastre
),

uuid_assigned as (
    select 
        *,
        uuid_generate_v5(
            'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'::uuid,
            concat_ws('||','cadastre',id::text)
        )
        as uuid
    from property_ref
),



deduplicated as (
    select distinct on (uuid) *
    from uuid_assigned
    order by uuid
),

property_ref_deduplicated as (
    select distinct on (property_ref) *
    from deduplicated
    order by property_ref
)

   
select *
from property_ref_deduplicated
