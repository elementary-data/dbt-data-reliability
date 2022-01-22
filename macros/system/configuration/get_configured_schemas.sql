{% macro get_configured_schemas() %}

    {% set configured_schemas_query %}

    with monitoring_configuration as (

        select {{ full_schema_name() }}
        from {{ configured_schemas_path() }}
        where alert_on_schema_changes = true
        group by 1
        union all
        select {{ full_schema_name() }}
        from {{ configured_tables_path() }}
        where alert_on_schema_changes = true
        group by 1
        union all
        select {{ full_schema_name() }}
        from {{ configured_columns_path() }}
        where alert_on_schema_changes = true
        group by 1

    )

    select distinct full_schema_name
    from monitoring_configuration

    {% endset %}

    {% set configured_schemas = result_column_to_list(configured_schemas_query) %}
    {{ return(configured_schemas) }}

{% endmacro %}