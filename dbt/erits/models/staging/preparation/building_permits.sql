{{ config(
    alias=named_model('building_permits')
)}}

{{ incremental_config(
    unique_key='fid',
    strategy='merge',
    schema='Silver',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('building_permits', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('building_permits', suffix='fid') }} ON {{ this }} (fid)",
        "ANALYZE {{ this }}"
    ]
) }}

WITH permits AS (
    {{ incremental_filter(source('bronze_source', 'building_permits'), 'fid', strategy='merge') }}
),

subcounty AS (
    SELECT geom AS subcounty_geom
    FROM {{ ref('subcounty_unit') }}
),

joined AS (
    SELECT 
        p.* 
    FROM permits p
    JOIN subcounty s
      ON ST_Intersects(p.geom, s.subcounty_geom)
)

SELECT DISTINCT ON (fid) *
FROM joined
