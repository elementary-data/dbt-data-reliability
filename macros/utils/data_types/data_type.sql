{% macro is_primitive(val) %}
  {% do return (
    val is none or
    val is boolean or
    val is number or
    val is string
  ) %}
{% endmacro %}

{%- macro type_bool() -%}
    {{ return(adapter.dispatch('type_bool', 'elementary')()) }}
{%- endmacro -%}

{% macro default__type_bool() %}
    {% do return("boolean") %}
{% endmacro %}

{% macro bigquery__type_bool() %}
    {% do return("BOOL") %}
{% endmacro %}


{%- macro type_string() -%}
    {{ return(adapter.dispatch('type_string', 'elementary')()) }}
{%- endmacro -%}

{% macro default__type_string() %}
    {# Redshift and Postgres #}
    {% do return("varchar(4096)") %}
{% endmacro %}

{% macro snowflake__type_string() %}
    {# Default max varchar size in Snowflake is 16MB #}
    {% do return("varchar") %}
{% endmacro %}

{% macro bigquery__type_string() %}
    {# Default max string size in Bigquery is 65K #}
    {% do return("string") %}
{% endmacro %}

{% macro spark__type_string() %}
    {% do return("string") %}
{% endmacro %}



{%- macro type_long_string() -%}
    {{ return(adapter.dispatch('type_long_string', 'elementary')()) }}
{%- endmacro -%}

{%- macro default__type_long_string() -%}
    {# Snowflake, Bigquery, Databricks #}
    {% do return(elementary.type_string()) %}
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
    {% set macro = dbt.type_bigint or dbt_utils.type_bigint %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_bigint` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}


{% macro type_float() %}
    {% set macro = dbt.type_float or dbt_utils.type_float %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_float` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}


{% macro type_int() %}
    {% set macro = dbt.type_int or dbt_utils.type_int %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_int` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}


{% macro type_timestamp() %}
    {% set macro = dbt.type_timestamp or dbt_utils.type_timestamp %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_timestamp` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}
