{{ config(
    alias=named_model('addresses'),
    pre_hook=[
        "CREATE EXTENSION IF NOT EXISTS  fuzzystrmatch"
    ]
) }}

{{ incremental_config(
    schema='Silver',
    unique_key='id',
    strategy='merge',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('addresses', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('addresses', suffix='id') }} ON {{ this }} (id)",
        "ANALYZE {{ this }}"
    ]
) }}

with addresses as (
    {{ incremental_filter(source('bronze_source', 'address'), 'id', strategy='merge') }}
),

subcounty as (
    select geom
    from {{ ref('subcounty_unit') }}
),

-- normalize accesstype (focus on 'primary' and close variants)
normalized as (
    select 
        a.*,
        case
             when a.accesstype is null then null

            -- direct matches
            when trim(lower(a.accesstype::text)) in ('primary', 'primary access') then 'primary'

            -- substring LIKE matches
            when lower(a.accesstype::text) like '%primary%' then 'primary'

            -- fuzzy close to "primary"
            when levenshtein(trim(lower(a.accesstype::text)), 'primary') <= 2 then 'primary'
            when levenshtein(trim(lower(a.accesstype::text)), 'primary access') <= 3 then 'primary'

            else null
        end as accesstype_norm
    from addresses a
    join subcounty s
      on ST_Intersects(a.geom, s.geom)
),

-- keep only primary
filtered as (
    select *
    from normalized
    where accesstype_norm = 'primary'
),

-- remove duplicates
deduped as (
    select distinct on (id) *
    from filtered
    order by id
)

select 
    id,
    geom,
    accesstype_norm as accesstype,
    {{ dbt_utils.star(from=source('bronze_source','address'), except=["accesstype","id","geom"]) }}
from deduped
