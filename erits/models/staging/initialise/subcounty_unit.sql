{{ config(
    schema='Silver',
    materialized='table',
    alias=named_model('subcounty_unit'),
    post_hook=[
     "CREATE INDEX IF NOT EXISTS {{ named_model('au_subcounty_unit', suffix='geom') }} ON {{ this }} USING GIST (geom)"
    ]
) }}

SELECT *
FROM {{ source('bronze_source', 'au_subcounty_unit') }}
WHERE scouname ILIKE '{{ var("area") }}'
