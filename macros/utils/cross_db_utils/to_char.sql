{% macro to_char(column, format) -%}
    {{ return(adapter.dispatch('to_char', 'elementary')(column, format)) }}
{%- endmacro %}

{# Snowflake and Redshift #}
{% macro default__to_char(column, format) %}
    to_char({{ column }} {%- if format %}, '{{ format }}'){%- else %}, 'YYYY-MM-DD HH:MI:SS'){%- endif %}
{% endmacro %}

{% macro bigquery__to_char(column, format) %}
    cast({{ column }} as STRING {%- if format %} FORMAT '{{ format }}'){%- else %}){%- endif %}
{% endmacro %}

{% macro databricks__to_char(column, format) %}
    date_format({{ column }} {%- if format %}, '{{ format }}'){%- else %}, 'YYYY-MM-DD HH:MI:SS'){%- endif %}
{% endmacro %}

{% macro spark__to_char(column, format) %}
    date_format({{ column }} {%- if format %}, '{{ format }}'){%- else %}, 'YYYY-MM-DD HH:MI:SS'){%- endif %}
{% endmacro %}