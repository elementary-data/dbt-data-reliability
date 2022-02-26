{%- macro type_bool() -%}
    {{ return(adapter.dispatch('type_bool', 'elementary_data_reliability')()) }}
    {%- endmacro -%}

{% macro default__type_bool() %}
    boolean
{% endmacro %}

{% macro bigquery__type_bool() %}
    BOOL
{% endmacro %}

