{%- macro type_bool() -%}
    {{ return(adapter.dispatch('type_bool', 'elementary')()) }}
{%- endmacro -%}

{% macro default__type_bool() %}
    boolean
{% endmacro %}

{% macro bigquery__type_bool() %}
    BOOL
{% endmacro %}


{# In our project we defined type_string different from dbt, so we have a new macro for it #}
{%- macro elementary_type_string() -%}
    {{ return(adapter.dispatch('elementary_type_string', 'elementary')()) }}
{%- endmacro -%}

{% macro default__elementary_type_string() %}
    {# Redshift and Postgres #}
    varchar(4096)
{% endmacro %}

{% macro snowflake__elementary_type_string() %}
    {# Default max varchar size in Snowflake is 16MB #}
    varchar
{% endmacro %}

{% macro bigquery__elementary_type_string() %}
    {# Default max string size in Bigquery is 65K #}
    string
{% endmacro %}

{% macro databricks__elementary_type_string() %}
    string
{% endmacro %}



{%- macro elementary_type_long_string() -%}
    {{ return(adapter.dispatch('elementary_type_long_string', 'elementary')()) }}
{%- endmacro -%}

{%- macro default__elementary_type_long_string() -%}
    {{ elementary.elementary_type_string() }}
{%- endmacro -%}

{%- macro redshift__elementary_type_long_string() -%}
    varchar(16384)
{%- endmacro -%}

{%- macro postgres__elementary_type_long_string() -%}
    varchar(16384)
{%- endmacro -%}


{% macro type_string() %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.type_string()) }}
    {% else %}
        {{ return(dbt_utils.type_string()) }}
    {% endif %}
{% endmacro %}


{% macro type_bigint() %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.type_bigint()) }}
    {% else %}
        {{ return(dbt_utils.type_bigint()) }}
    {% endif %}
{% endmacro %}


{% macro type_float() %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.type_float()) }}
    {% else %}
        {{ return(dbt_utils.type_float()) }}
    {% endif %}
{% endmacro %}


{% macro type_int() %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.type_int()) }}
    {% else %}
        {{ return(dbt_utils.type_int()) }}
    {% endif %}
{% endmacro %}


{% macro type_timestamp() %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.type_timestamp()) }}
    {% else %}
        {{ return(dbt_utils.type_timestamp()) }}
    {% endif %}
{% endmacro %}
