{%- macro type_bool() -%}
    {{ return(adapter.dispatch('type_bool', 'elementary_data_reliability')()) }}
    {%- endmacro -%}

    {% macro default__type_bool() %}
    boolean
{% endmacro %}

{% macro bigquery__type_bool() %}
    BOOL
{% endmacro %}

{# TODO: not sure if this works for postgres as it needs a type, maybe add macro to cast or use json? #}
{%- macro type_array() -%}
    {{ return(adapter.dispatch('type_array', 'elementary_data_reliability')()) }}
{%- endmacro -%}

{% macro default__type_array() %}
    array
{% endmacro %}

{%- macro redshift__type_array() -%}
    super
{%- endmacro -%}


{# TODO: not sure if this works for postgres as it needs a type, maybe add macro to cast #}
{%- macro type_json() -%}
    {{ return(adapter.dispatch('type_json', 'elementary_data_reliability')()) }}
{%- endmacro -%}

{% macro default__type_json() %}
    json
{% endmacro %}

{% macro snowflake__type_json() %}
    VARIANT
{% endmacro %}

{% macro bigquery__type_json() %}
    JSON
{% endmacro %}

{%- macro redshift__type_json() -%}
    super
{%- endmacro -%}

{%- macro postgres__type_json() -%}
    json
{%- endmacro -%}