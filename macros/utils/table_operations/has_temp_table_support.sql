{% macro has_temp_table_support() %}
    {% do return(adapter.dispatch("has_temp_table_support", "elementary")()) %}
{% endmacro %}

{% macro default__has_temp_table_support() %} {% do return(true) %} {% endmacro %}

{% macro spark__has_temp_table_support() %} {% do return(false) %} {% endmacro %}

{% macro fabricspark__has_temp_table_support() %} {% do return(false) %} {% endmacro %}

{% macro trino__has_temp_table_support() %} {% do return(false) %} {% endmacro %}

{% macro athena__has_temp_table_support() %} {% do return(false) %} {% endmacro %}

{% macro dremio__has_temp_table_support() %} {% do return(false) %} {% endmacro %}

{% macro clickhouse__has_temp_table_support() %}
    {# ClickHouse CREATE TEMPORARY TABLE is session-scoped (Memory engine only,
       no database qualification).  The dbt-clickhouse adapter does not guarantee
       session persistence across execute() calls, so a temp table created in one
       statement may not be visible in the next.  Elementary's intermediate
       relations need cross-statement visibility and MergeTree engine, so we fall
       back to regular tables with cleanup instead. #}
    {% do return(false) %}
{% endmacro %}


{% macro redshift__has_temp_table_support() %}
    {# dbt-fusion uses connection pooling, so temp tables created in one session
       are not visible in other sessions, causing "relation does not exist" errors.
       Use regular tables with cleanup instead. #}
    {% if elementary.is_dbt_fusion() %} {% do return(false) %}
    {% else %} {% do return(true) %}
    {% endif %}
{% endmacro %}
