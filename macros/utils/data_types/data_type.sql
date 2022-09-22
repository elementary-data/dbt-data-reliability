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

{% macro databricks__type_string() %}
    string
{% endmacro %}

{% macro spark__type_string() %}
    string
{% endmacro %}



{%- macro type_long_string() -%}
    {{ return(adapter.dispatch('type_long_string', 'elementary')()) }}
{%- endmacro -%}

{%- macro default__type_long_string() -%}
    {# Snowflake, Bigquery, Databricks #}
    {{ elementary.type_string() }}
{%- endmacro -%}

{%- macro redshift__type_long_string() -%}
    {% set long_string = 'varchar(' ~ elementary.get_config_var('long_string_size') ~ ')' %}
    {{ return(long_string) }}
{%- endmacro -%}

{%- macro postgres__type_long_string() -%}
    {% set long_string = 'varchar(' ~ elementary.get_config_var('long_string_size') ~ ')' %}
    {{ return(long_string) }}
{%- endmacro -%}


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
