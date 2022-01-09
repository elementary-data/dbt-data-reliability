-- TODO: support include and exclude tables and schemas
-- TODO: add validation that tables exist in the db

{% macro get_monitored_full_table_names() %}
    {% set monitoring_configuration_query %}
        select distinct upper(full_table_name) as full_table_name
        from {{ var('elementary')['columns_monitoring_configuration'] }}
        where monitored = true
    {% endset %}
    {% set monitored_full_table_names = column_to_list(monitoring_configuration_query) %}
    {{ return(monitored_full_table_names) }}
{% endmacro %}


{% macro get_monitored_dbs() %}

    {% set monitored_dbs_query %}
        select distinct
             upper(db_name) as db_name
        from {{ var('elementary')['schemas_monitoring_configuration'] }}
    {% endset %}

    {% set monitored_dbs = column_to_list(monitored_dbs_query) %}
    {{ return(monitored_dbs) }}

{% endmacro %}