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
    stddev(cast({{ column_name }} as {{ elementary.type_float() }}))
{%- endmacro %}

{% macro variance(column_name) -%}
    variance(cast({{ column_name }} as {{ elementary.type_float() }}))
{%- endmacro %}
