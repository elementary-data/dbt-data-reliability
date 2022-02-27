{% macro column_quote(column_name) %}
    {{ adapter.dispatch('column_quote')(column_name) }}
{% endmacro %}

{% macro default__column_quote(column_name) %}
    "{{ column_name }}"
{% endmacro %}

{% macro bigquery__column_quote(column_name) %}
    `{{ column_name }}`
{% endmacro %}