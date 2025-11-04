{% macro named_model(model_name, prefix=var('area', default='langata'), suffix=None) %}
  {% set parts = [] %}

  {% if prefix %}
    {% set clean_prefix = prefix | replace("'", "") | lower %}
    {% do parts.append(clean_prefix) %}
  {% endif %}

  {% set clean_model_name = model_name | replace("'", "") | lower %}
  {% do parts.append(clean_model_name) %}

  {% if suffix %}
    {% set clean_suffix = suffix | replace("'", "") | lower %}
    {% do parts.append(clean_suffix) %}
  {% endif %}

  {{ return(parts | join('_')) }}
{% endmacro %}
