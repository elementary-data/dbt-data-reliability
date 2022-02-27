{% macro max_length_monitor(column_name) -%}
    max(length({{ column_name }}))
{%- endmacro %}

{% macro min_length_monitor(column_name) -%}
    min(length({{ column_name }}))
{%- endmacro %}

{% macro missing_count_monitor(column_name) %}
   coalesce(sum(case when {{ column_name }} is null then 1 when {{ column_name }} = '' then 1 when lower({{ column_name }}) = 'null' then 1 else 0 end), 0)
{% endmacro %}

{% macro missing_percent_monitor(column_name) %}
    {{ percent(missing_count_monitor(column_name), row_count_monitor()) }}
{% endmacro %}