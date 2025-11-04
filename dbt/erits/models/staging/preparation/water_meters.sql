{{ config(
    alias=named_model('water_meters')
)}}


{{ incremental_config(
    unique_key='fid',
    strategy='merge',
    schema='Silver',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('water_meters', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('water_meters', suffix='fid') }} ON {{ this }} (fid)",
        "ANALYZE {{ this }}"
    ]
) }}

WITH water AS (
    {{ incremental_filter(source('bronze_source', 'water_meters'), 'fid', strategy='merge') }}
),

subcounty AS (
    SELECT geom AS subcounty_geom
    FROM {{ ref('subcounty_unit') }}
),

joined AS (
    SELECT
        w.*
    FROM water w
    JOIN subcounty s
      ON ST_Intersects(w.geom, s.subcounty_geom)
)

SELECT DISTINCT ON (fid) *
FROM joined
