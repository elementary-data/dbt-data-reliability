{% macro has_temp_table_support() %}
    {% do return(adapter.dispatch("has_temp_table_support", "elementary")()) %}
{% endmacro %}

{% macro default__has_temp_table_support() %}
    {% do return(true) %}
{% endmacro %}

{% macro spark__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

{% macro trino__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

{% macro athena__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

{% macro dremio__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

{% macro clickhouse__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

{% macro redshift__has_temp_table_support() %}
    {# dbt-fusion uses connection pooling, so temp tables created in one session
       are not visible in other sessions, causing "relation does not exist" errors.
       Use regular tables with cleanup instead. #}
    {% if elementary.is_dbt_fusion() %}
        {% do return(false) %}
    {% else %}
        {% do return(true) %}
    {% endif %}
{% endmacro %}

