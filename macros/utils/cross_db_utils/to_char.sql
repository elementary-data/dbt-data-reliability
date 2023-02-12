{% macro edr_to_char(column, format) -%}
    {{ return(adapter.dispatch('edr_to_char', 'elementary')(column, format)) }}
{%- endmacro %}

{# Snowflake and Redshift/Postgres #}
{% macro default__edr_to_char(column, format) %}
    to_char({{ column }} {%- if format %}, '{{ format }}'){%- else %}, 'YYYY-MM-DD HH:MI:SS'){%- endif %}
{% endmacro %}

{% macro bigquery__edr_to_char(column, format) %}
    cast({{ column }} as STRING {%- if format %} FORMAT '{{ format }}'){%- else %}){%- endif %}
{% endmacro %}

{% macro spark__edr_to_char(column, format) %}
    date_format({{ column }} {%- if format %}, '{{ format }}'){%- else %}, 'YYYY-MM-DD HH:MI:SS'){%- endif %}
{% endmacro %}