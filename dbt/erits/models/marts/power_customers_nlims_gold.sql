{{config(alias=named_model('power_customers_nlims_gold'))}}


{{ incremental_config(
    schema = 'gold',
    strategy = 'merge',
    unique_key = 'fid',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('power_customers_nlims_gold', suffix='fid') }} ON {{ this }} (fid)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('power_customers_nlims_gold', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

with base as (
    {{ incremental_filter(ref('power_customers_nlims_link'), 'fid', strategy='merge') }}
)

select DISTINCT ON (fid) * from base