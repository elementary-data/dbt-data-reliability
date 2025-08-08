{# We create tables and some databases limit the length of table names #}
{% macro get_relation_max_name_length() %}
    {{ return(adapter.dispatch('get_relation_max_name_length', 'elementary')()) }}
{% endmacro %}

{% macro default__get_relation_max_name_length(temporary, relation, sql_query) %}
    {{ return(none) }}
{% endmacro %}

{% macro snowflake__get_relation_max_name_length(temporary, relation, sql_query) %}
    {{ return(255) }}
{% endmacro %}

{% macro redshift__get_relation_max_name_length(temporary, relation, sql_query) %}
    {{ return(125) }}
{% endmacro %}

{% macro postgres__get_relation_max_name_length(temporary, relation, sql_query) %}
    {{ return(63) }}
{% endmacro %}

{% macro spark__get_relation_max_name_length(temporary, relation, sql_query) %}
    {{ return(127) }}
{% endmacro %}

{% macro athena__get_relation_max_name_length(temporary, relation, sql_query) %}
    {{ return(255) }}
{% endmacro %}

{% macro trino__get_relation_max_name_length(temporary, relation, sql_query) %}
    {{ return(128) }}
{% endmacro %}

{% macro clickhouse__get_relation_max_name_length(temporary, relation, sql_query) %}
    {{ return(128) }}
{% endmacro %}

{% macro dremio__get_relation_max_name_length(temporary, relation, sql_query) %}
    {{ return(128) }}
{% endmacro %}