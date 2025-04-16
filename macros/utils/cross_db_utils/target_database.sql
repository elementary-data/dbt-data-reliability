{% macro target_database() -%}
    {{ return(adapter.dispatch('target_database', 'elementary')()) }}
{%- endmacro %}

-- Postgres and Redshift
{% macro default__target_database() %}
    {% do return(target.dbname) %}
{% endmacro %}

{% macro spark__target_database() %}
    {% do return(target.catalog or none) %}
{% endmacro %}

{% macro snowflake__target_database() %}
    {% do return(target.database) %}
{% endmacro %}

{% macro bigquery__target_database() %}
    {% do return(target.project) %}
{% endmacro %}

{% macro athena__target_database() %}
    {% do return(target.database) %}
{% endmacro %}

{% macro trino__target_database() %}
    {% do return(target.database) %}
{% endmacro %}

{% macro clickhouse__target_database() %}
    {% do return(target.schema) %}
{% endmacro %}
