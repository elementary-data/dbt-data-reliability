{% macro length(column_name) -%}
    {{ return(adapter.dispatch('length', 'elementary') (column_name)) }}
{%- endmacro %}

{% macro default__length(column_name) -%}
    length({{ column_name }})
{%- endmacro %}

{% macro sqlserver__length(column_name) -%}
    len({{ column_name }})
{%- endmacro %}


{% macro trim(column_name, space_string) -%}
    {{ return(adapter.dispatch('trim', 'elementary') (column_name, space_string)) }}
{%- endmacro %}

{% macro default__trim(column_name, space_string) -%}
    trim({{ column_name }}, space_string)
{%- endmacro %}

{% macro sqlserver__trim(column_name, space_string) -%}
    trim({{ column_name }})
{%- endmacro %}


{% macro max_length(column_name) -%}
    max({{ elementary.length(column_name) }})
{%- endmacro %}

{% macro min_length(column_name) -%}
    min({{ elementary.length(column_name) }})
{%- endmacro %}

{% macro average_length(column_name) -%}
    avg({{ elementary.length(column_name) }})
{%- endmacro %}

{% macro missing_count(column_name) %}
    coalesce(sum(case when {{ column_name }} is null then 1 when {{ elementary.trim(column_name, ' ') }} = '' then 1 when lower({{ column_name }}) = 'null' then 1 else 0 end), 0)
{% endmacro %}

{% macro missing_percent(column_name) %}
    {{ elementary.percent(elementary.missing_count(column_name), elementary.row_count()) }}
{% endmacro %}
