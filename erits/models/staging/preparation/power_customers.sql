{{ config(
    alias=named_model('power_customers')
)}}

{{ incremental_config(
    unique_key='fid',
    strategy='merge',
    schema='Silver',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('power_customers', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('power_customers', suffix='fid') }} ON {{ this }} (fid)",
        "ANALYZE {{ this }}"
    ]
) }}

WITH power AS (
    {{ incremental_filter(source('bronze_source', 'power_customers'), 'fid', strategy='merge') }}
),

subcounty AS (
    SELECT geom AS subcounty_geom
    FROM {{ ref('subcounty_unit') }}
),

joined AS ( 
    SELECT
        p.*
    FROM power p
    JOIN subcounty s
      ON ST_Intersects(p.geom, s.subcounty_geom)
)

SELECT DISTINCT ON (fid) *
FROM joined
