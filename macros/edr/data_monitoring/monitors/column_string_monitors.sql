{% macro max_length(column_name) -%}
    max(length({{ column_name }}))
{%- endmacro %}

{% macro min_length(column_name) -%}
    min(length({{ column_name }}))
{%- endmacro %}

{% macro average_length(column_name) -%}
    avg(length({{ column_name }}))
{%- endmacro %}

{% macro missing_count(column_name) %}
    coalesce(sum(case when {{ column_name }} is null then 1 when trim({{ column_name }}) = '' then 1 when lower({{ column_name }}) = 'null' then 1 else 0 end), 0)
{% endmacro %}

{% macro missing_percent(column_name) %}
    {{ elementary.edr_percent(elementary.missing_count(column_name), elementary.row_count()) }}
{% endmacro %}

{% macro not_missing_percent(column_name) %}
    {{ elementary.edr_not_percent(elementary.missing_count(column_name), elementary.row_count()) }}
{% endmacro %}
