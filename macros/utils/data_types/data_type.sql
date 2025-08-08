{% macro is_primitive(val) %}
  {% do return (
    val is none or
    val is boolean or
    val is number or
    val is string
  ) %}
{% endmacro %}

{%- macro edr_type_bool() -%}
    {{ return(adapter.dispatch('edr_type_bool', 'elementary')()) }}
{%- endmacro -%}

{% macro default__edr_type_bool() %}
    {% do return("boolean") %}
{% endmacro %}

{% macro bigquery__edr_type_bool() %}
    {% do return("BOOL") %}
{% endmacro %}


{%- macro edr_type_string() -%}
    {{ return(adapter.dispatch('edr_type_string', 'elementary')()) }}
{%- endmacro -%}

{% macro default__edr_type_string() %}
    {# Redshift #}
    {% do return("varchar(4096)") %}
{% endmacro %}

{% macro postgres__edr_type_string() %}
    {% if var("sync", false) %}
        {% do return("text") %}
    {% else %}
        {% do return("varchar(4096)") %}
    {% endif %}
{% endmacro %}

{% macro clickhouse__edr_type_string() %}
    {% do return("String") %}
{% endmacro %}

{% macro snowflake__edr_type_string() %}
    {# Default max varchar size in Snowflake is 16MB #}
    {% do return("varchar") %}
{% endmacro %}

{% macro bigquery__edr_type_string() %}
    {# Default max string size in Bigquery is 65K #}
    {% do return("string") %}
{% endmacro %}

{% macro spark__edr_type_string() %}
    {% do return("string") %}
{% endmacro %}

{% macro athena__edr_type_string() %}
    {% do return("varchar") %}
{% endmacro %}

{% macro trino__edr_type_string() %}
    {% do return("varchar") %}
{% endmacro %}




{%- macro edr_type_long_string() -%}
    {{ return(adapter.dispatch('edr_type_long_string', 'elementary')()) }}
{%- endmacro -%}

{%- macro default__edr_type_long_string() -%}
    {# Snowflake, Bigquery, Databricks #}
    {% do return(elementary.edr_type_string()) %}
{%- endmacro -%}

{%- macro redshift__edr_type_long_string() -%}
    {% set long_string = 'varchar(' ~ elementary.get_config_var('long_string_size') ~ ')' %}
    {{ return(long_string) }}
{%- endmacro -%}

{%- macro postgres__edr_type_long_string() -%}
    {% set long_string = 'text' %}
    {{ return(long_string) }}
{%- endmacro -%}


{% macro edr_type_bigint() %}
    {% set macro = dbt.type_bigint or dbt_utils.type_bigint %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_bigint` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}


{% macro edr_type_float() %}
    {% set macro = dbt.type_float or dbt_utils.type_float %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_float` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}


{% macro edr_type_int() %}
    {% set macro = dbt.type_int or dbt_utils.type_int %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_int` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}


{% macro edr_type_timestamp() %}
    {{ return(adapter.dispatch('edr_type_timestamp', 'elementary')()) }}
{% endmacro %}

{% macro default__edr_type_timestamp() %}
    {% set macro = dbt.type_timestamp or dbt_utils.type_timestamp %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_timestamp` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}


{% macro edr_type_numeric() %}
    {% set macro = dbt.type_numeric or dbt_utils.type_numeric %}
    {% if not macro %}
        {{ exceptions.raise_compiler_error("Did not find a `type_numeric` macro.") }}
    {% endif %}
    {{ return(macro()) }}
{% endmacro %}


{% macro edr_type_date() %}
    {{ return(adapter.dispatch('edr_type_date', 'elementary')()) }}
{% endmacro %}

{% macro default__edr_type_date() %}
    date
{% endmacro %}

{% macro athena__edr_type_timestamp() %}
  {%- set config = model.get('config', {}) -%}
  {%- set table_type = config.get('table_type', 'hive') -%}
  {%- if table_type == 'iceberg' -%}
    timestamp(6)
  {%- else -%}
    timestamp
  {%- endif -%}
{% endmacro %}

{% macro trino__edr_type_timestamp() %}
    timestamp(6)
{% endmacro %}

{% macro dremio__edr_type_timestamp() %}
    timestamp
{% endmacro %}
