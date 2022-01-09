-- TODO: support include and exclude tables and schemas
-- TODO: add validation that tables exist in the db

{% macro get_monitored_full_table_names() %}
    {% set monitoring_configuration_query %}
        select distinct upper(full_table_name) as full_table_name
        from {{ var('elementary')['monitoring_configuration_table'] }}
        where monitored = true
    {% endset %}
    {% set monitored_full_table_names = column_to_list(monitoring_configuration_query) %}
    {{ return(monitored_full_table_names) }}
{% endmacro %}


{% macro get_monitored_dbs() %}
    {% set monitored_full_table_names = get_monitored_full_table_names() %}
    {% set monitored_dbs = [] %}
    {% for full_table_name in monitored_full_table_names %}
        {% set split_table_name = full_table_name.split('.') %}
        {{ monitored_dbs.append(split_table_name[0]) }}
    {% endfor %}
    {% set monitored_dbs = monitored_dbs|unique|list %}
    {{ return(monitored_dbs) }}
{% endmacro %}