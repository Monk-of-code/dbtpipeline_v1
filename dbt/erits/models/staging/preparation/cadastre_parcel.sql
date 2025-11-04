{{ config(
    alias=named_model('cadastral_parcel')
)}}


{{ incremental_config(
    unique_key='id',
    strategy='merge',
    schema='Silver',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('cadastral_parcel', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('cadastral_parcel', suffix='fid') }} ON {{ this }} (id)",
        "ANALYZE {{ this }}"
    ]
) }}

WITH parcel AS (
    {{ incremental_filter(source('bronze_source', 'cp_cadastral_parcel'), 'id', strategy='merge') }}
),

subcounty AS (
    SELECT geom AS subcounty_geom
    FROM {{ ref('subcounty_unit') }}
),

joined AS (
    SELECT
        p.*
    FROM parcel p
    JOIN subcounty s
      ON ST_Intersects(p.geom, s.subcounty_geom)
),

active AS (
    SELECT *
    FROM joined
    WHERE current_status = 'Active'
)

SELECT DISTINCT ON (id) *
FROM active
