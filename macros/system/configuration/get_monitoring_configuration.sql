-- TODO: add validation that tables exist in the db

{% macro get_monitored_dbs() %}

    {% set monitored_dbs_query %}
        select distinct upper(db_name) as db_name
        from {{ get_schemas_configuration() }}
    {% endset %}

    {% set monitored_dbs = column_to_list(monitored_dbs_query) %}
    {{ return(monitored_dbs) }}

{% endmacro %}



{% macro get_monitored_schemas() %}

    {% set monitored_schemas_query %}
        select distinct
        upper(concat(db_name, '.', schema_name)) as schemas_full_names
        from {{ get_schemas_configuration() }}
        group by 1
    {% endset %}

    {% set monitored_schemas = column_to_list(monitored_schemas_query) %}
    {{ return(monitored_schemas) }}

{% endmacro %}