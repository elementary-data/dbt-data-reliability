{% macro max_monitor(column_name) -%}
    max(cast({{ column_name }} as {{ dbt_utils.type_int() }}))
{%- endmacro %}

{% macro min_monitor(column_name) -%}
    min(cast({{ column_name }} as {{ dbt_utils.type_int() }}))
{%- endmacro %}

{% macro average_monitor(column_name) -%}
    avg(cast({{ column_name }} as {{ dbt_utils.type_int() }}))
{%- endmacro %}

{% macro zero_count_monitor(column_name) %}
    coalesce(sum(case when {{ column_name }} is null then 1 when cast({{ column_name }} as {{ dbt_utils.type_int() }}) = 0 then 1 else 0 end), 0)
{% endmacro %}

{% macro zero_percent_monitor(column_name) %}
    {{ percent(zero_count_monitor(column_name), row_count_monitor()) }}
{% endmacro %}

{% macro standard_deviation_monitor(column_name) -%}
    stddev(cast({{ column_name }} as {{ dbt_utils.type_int() }}))
{%- endmacro %}

{% macro variance_monitor(column_name) -%}
    variance(cast({{ column_name }} as {{ dbt_utils.type_int() }}))
{%- endmacro %}
