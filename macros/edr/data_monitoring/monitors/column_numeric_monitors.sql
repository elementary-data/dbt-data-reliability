{% macro max(column_name) -%}
    max(cast({{ column_name }} as {{ elementary.type_float() }}))
{%- endmacro %}

{% macro min(column_name) -%}
    min(cast({{ column_name }} as {{ elementary.type_float() }}))
{%- endmacro %}

{% macro average(column_name) -%}
    avg(cast({{ column_name }} as {{ elementary.type_float() }}))
{%- endmacro %}

{% macro zero_count(column_name) %}
    coalesce(sum(case when {{ column_name }} is null then 1 when cast({{ column_name }} as {{ elementary.type_float() }}) = 0 then 1 else 0 end), 0)
{% endmacro %}

{% macro zero_percent(column_name) %}
    {{ elementary.percent(elementary.zero_count(column_name), elementary.row_count()) }}
{% endmacro %}

{% macro standard_deviation(column_name) -%}
    {{ elementary.stddev(elementary.cast_as_float(column_name)) }}
{%- endmacro %}

{% macro variance(column_name) -%}
    {{ return(adapter.dispatch('variance', 'elementary') (column_name)) }}
{%- endmacro %}

{% macro default__variance(column_name) -%}
    variance(cast({{ column_name }} as {{ elementary.type_float() }}))
{%- endmacro %}

{% macro sqlserver__variance(column_name) -%}
    var(cast({{ column_name }} as {{ elementary.type_float() }}))
{%- endmacro %}