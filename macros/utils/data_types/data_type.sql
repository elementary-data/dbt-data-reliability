{% macro is_primitive(val) %}
  {% do return (
    val is none or
    val is boolean or
    val is number or
    val is string
  ) %}
{% endmacro %}

{%- macro elementary_type_bool() -%}
    {{ return(adapter.dispatch('elementary_type_bool', 'elementary')()) }}
{%- endmacro -%}

{% macro default__elementary_type_bool() %}
    {% do return("boolean") %}
{% endmacro %}

{% macro bigquery__elementary_type_bool() %}
    {% do return("BOOL") %}
{% endmacro %}


{%- macro elementary_type_string() -%}
    {{ return(adapter.dispatch('elementary_type_string', 'elementary')()) }}
{%- endmacro -%}

{% macro default__elementary_type_string() %}
    {# Redshift and Postgres #}
    {% do return("varchar(4096)") %}
{% endmacro %}

{% macro snowflake__elementary_type_string() %}
    {# Default max varchar size in Snowflake is 16MB #}
    {% do return("varchar") %}
{% endmacro %}

{% macro bigquery__elementary_type_string() %}
    {# Default max string size in Bigquery is 65K #}
    {% do return("string") %}
{% endmacro %}

{% macro spark__elementary_type_string() %}
    {% do return("string") %}
{% endmacro %}



{%- macro elementary_type_long_string() -%}
    {{ return(adapter.dispatch('elementary_type_long_string', 'elementary')()) }}
{%- endmacro -%}

{%- macro default__elementary_type_long_string() -%}
    {# Snowflake, Bigquery, Databricks #}
    {% do return(elementary.elementary_type_string()) %}
{%- endmacro -%}

{%- macro redshift__elementary_type_long_string() -%}
    {% set long_string = 'varchar(' ~ elementary.get_config_var('long_string_size') ~ ')' %}
    {{ return(long_string) }}
{%- endmacro -%}

{%- macro postgres__elementary_type_long_string() -%}
    {% set long_string = 'varchar(' ~ elementary.get_config_var('long_string_size') ~ ')' %}
    {{ return(long_string) }}
{%- endmacro -%}


{% macro elementary_type_bigint() %}
    {% set macro = dbt.type_bigint or dbt_utils.type_bigint %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_bigint` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}


{% macro elementary_type_float() %}
    {% set macro = dbt.type_float or dbt_utils.type_float %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_float` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}


{% macro elementary_type_int() %}
    {% set macro = dbt.type_int or dbt_utils.type_int %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_int` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}


{% macro elementary_type_timestamp() %}
    {% set macro = dbt.type_timestamp %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_timestamp` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}
