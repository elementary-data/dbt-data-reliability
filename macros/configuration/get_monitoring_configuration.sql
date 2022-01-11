-- TODO: add validation that tables exist in the db

{% macro get_monitored_dbs() %}

    {% set schemas_monitoring_configuration %}
        {{ target.database ~"."~ target.schema ~"."~ var('elementary')['schemas_monitoring_configuration']}}
    {% endset %}

    {% set monitored_dbs_query %}
        select distinct upper(db_name) as db_name
        from {{ schemas_monitoring_configuration }}
    {% endset %}

    {% set monitored_dbs = column_to_list(monitored_dbs_query) %}
    {{ return(monitored_dbs) }}

{% endmacro %}



{% macro get_monitored_schemas() %}

    {% set schemas_monitoring_configuration %}
        {{ target.database ~"."~ target.schema ~"."~ var('elementary')['schemas_monitoring_configuration']}}
    {% endset %}

    {% set monitored_schemas_query %}
        select distinct
        upper(concat(db_name, '.', schema_name)) as schemas_full_names
        from {{ schemas_monitoring_configuration }}
        group by 1
    {% endset %}

    {% set monitored_schemas = column_to_list(monitored_schemas_query) %}
    {{ return(monitored_schemas) }}

{% endmacro %}