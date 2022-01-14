-- TODO: add validation that tables exist in the db

{% macro get_monitored_dbs() %}

    {% set monitored_dbs_query %}
        select distinct upper(database_name) as database_name
        from {{ schemas_configuration_table() }}
    {% endset %}

    {% set monitored_dbs = column_to_list(monitored_dbs_query) %}
    {{ return(monitored_dbs) }}

{% endmacro %}



{% macro get_monitored_schemas() %}

    {% set monitored_schemas_query %}
        select distinct
        {{ full_schema_name() }}
        from {{ schemas_configuration_table() }}
        group by 1
    {% endset %}

    {% set monitored_schemas = column_to_list(monitored_schemas_query) %}
    {{ return(monitored_schemas) }}

{% endmacro %}