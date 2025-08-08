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

