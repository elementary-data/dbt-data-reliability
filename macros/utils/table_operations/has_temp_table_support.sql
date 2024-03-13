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

