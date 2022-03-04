{% macro target_database() -%}
    {{ return(adapter.dispatch('target_database', 'elementary')()) }}
{%- endmacro %}

-- Postgres and Redshift
{% macro default__target_database() %}
    {% set database = target.dbname %}
    {{ return(database) }}
{% endmacro %}

{% macro snowflake__target_database() %}
    {% set database = target.database %}
    {{ return(database) }}
{% endmacro %}

{% macro bigquery__target_database() %}
    {% set database = target.project %}
    {{ return(database) }}
{% endmacro %}
