{%- macro type_bool() -%}
    {{ return(adapter.dispatch('type_bool', 'elementary')()) }}
{%- endmacro -%}

{% macro default__type_bool() %}
    boolean
{% endmacro %}

{% macro bigquery__type_bool() %}
    BOOL
{% endmacro %}


{%- macro type_string() -%}
    {{ return(adapter.dispatch('type_string', 'elementary')()) }}
{%- endmacro -%}

{% macro default__type_string() %}
    varchar
{% endmacro %}

{% macro bigquery__type_string() %}
    string
{% endmacro %}

{% macro redshift__type_string() %}
    varchar(256)
{% endmacro %}


{%- macro type_long_string() -%}
    {{ return(adapter.dispatch('type_long_string', 'elementary')()) }}
{%- endmacro -%}

{%- macro default__type_long_string() -%}
    {{ elementary.type_string() }}
{%- endmacro -%}

{%- macro redshift__type_long_string() -%}
    varchar(4096)
{%- endmacro -%}
