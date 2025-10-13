{{ config(
    alias=named_model('buildings')
)}}


{{ incremental_config(
    unique_key='id',
    strategy='merge',
    schema='Silver',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('buildings', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('buildings', suffix='fid') }} ON {{ this }} (id)",
        "ANALYZE {{ this }}"
    ]
) }}

WITH buildings AS (
    {{ incremental_filter(source('bronze_source', 'buildings_183'), 'id', strategy='merge') }}
),

subcounty AS (
    SELECT geom AS subcounty_geom
    FROM {{ ref('subcounty_unit')}}
),

joined AS (
    SELECT 
        b.* 
    FROM buildings b
    JOIN subcounty s
      ON ST_Intersects(b.geom, s.subcounty_geom)
)

SELECT DISTINCT ON (id) *
FROM joined
