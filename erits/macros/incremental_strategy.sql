{# ----------------------------------------
   Unified Incremental Config
----------------------------------------- #}
{% macro incremental_config(
    unique_key='uuid',
    strategy='merge',
    schema=none,
    post_hook=[]
) %}
    {{ config(
        materialized='incremental',
        unique_key=unique_key,
        on_schema_change='sync_all_columns',
        incremental_strategy=strategy,
        schema=schema,
        post_hook=post_hook
    ) }}
{% endmacro %}


{# ----------------------------------------
   Incremental Filter (unchanged)
----------------------------------------- #}
{% macro incremental_filter(
    table_ref,
    unique_key='uuid',
    updated_at_column=none,
    strategy='merge'
) %}
    {% if is_incremental() %}
        {% if strategy == 'merge' %}
            select * from {{ table_ref }}

        {% elif strategy == 'append' %}
            select * from {{ table_ref }}
            where {{ unique_key }} not in (
                select {{ unique_key }} from {{ this }}
            )

        {% elif strategy == 'delete+insert' %}
            select * from {{ table_ref }}

        {% elif strategy == 'insert_overwrite' %}
            select * from {{ table_ref }}

        {% elif strategy == 'microbatch' %}
            select * from {{ table_ref }}
            {% if updated_at_column %}
              where {{ updated_at_column }} > (
                  select coalesce(max({{ updated_at_column }}), '1900-01-01'::timestamp)
                  from {{ this }}
              )
            {% endif %}
        {% endif %}
    {% else %}
        select * from {{ table_ref }}
    {% endif %}
{% endmacro %}
