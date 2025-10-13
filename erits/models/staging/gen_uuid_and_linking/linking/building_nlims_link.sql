{{ config(alias=named_model('building_nlims_link'))}}

{{ incremental_config(
    schema = 'Silver',
    strategy = 'merge',
    unique_key= 'id',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ named_model('building_nlims_link', suffix='geom') }} ON {{ this }} USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS {{ named_model('building_nlims_link', suffix='uuid') }} ON {{ this }} (uuid)",
        "ANALYZE {{ this }}"
    ]
) }}

with layer as (
    {{ incremental_filter(ref('buildings'), 'id', strategy='merge') }}
),

cadastre as (
    {{ incremental_filter(ref('cadastre_uuid'), 'uuid', strategy='merge') }}
),

joined as (
    select 
        layer.*, 
        cadastre.uuid ,
        cadastre.property_ref
    from layer
    left join cadastre
    on  cadastre.geom && layer.geom
    and ST_Intersects(layer.geom, cadastre.geom)
)

select DISTINCT ON (id) *
from joined
