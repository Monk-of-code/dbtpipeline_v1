{{config(alias=named_model('cp_kplc_nwsc_bldg_permits_address_gold'))}}


{{ incremental_config(
    schema = 'gold',
    strategy = 'merge',
    unique_key = 'uuid',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('cp_k_n_bldg_pts_add_gold', suffix='uuid') }} ON {{ this }} (uuid)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('cp_k_n_bldg_pts_add_gold', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

with base as (
    {{ incremental_filter(ref('cadastre_landrates_tradecount_property'), 'uuid', strategy='merge') }}
)

select DISTINCT ON (uuid) * from base