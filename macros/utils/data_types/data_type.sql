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
    {# Redshift and Postgres #}
    varchar(4096)
{% endmacro %}

{% macro snowflake__type_string() %}
    {# Default max varchar size in Snowflake is 16MB #}
    varchar
{% endmacro %}

{% macro bigquery__type_string() %}
    {# Default max string size in Bigquery is 65K #}
    string
{% endmacro %}




{%- macro type_long_string() -%}
    {{ return(adapter.dispatch('type_long_string', 'elementary')()) }}
{%- endmacro -%}

{%- macro default__type_long_string() -%}
    {{ elementary.type_string() }}
{%- endmacro -%}

{%- macro redshift__type_long_string() -%}
    varchar(65535)
{%- endmacro -%}

{%- macro postgres__type_long_string() -%}
    varchar(65535)
{%- endmacro -%}