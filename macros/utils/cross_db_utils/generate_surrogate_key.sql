{% macro generate_surrogate_key(field_list) -%}
  {% if dbt_version >= '1.3.0' %}
        {{ return(dbt_utils.generate_surrogate_key(field_list)) }}
    {% else %}
        {# This macro is abolition from dbt_utils version 1.0.0 #}
        {{ return(dbt_utils.surrogate_key(field_list)) }}
    {% endif %}
{%- endmacro %}
