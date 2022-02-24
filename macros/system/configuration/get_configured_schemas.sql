{% macro get_configured_schemas() %}

    {% set configured_schemas_query %}

    with monitoring_configuration as (

        select {{ full_schema_name() }} as full_schema_name
        from {{ var('tables_config') }}
        group by 1

    )

    select distinct full_schema_name
    from monitoring_configuration

    {% endset %}

    {% set configured_schemas = result_column_to_list(configured_schemas_query) %}
    {{ return(configured_schemas) }}

{% endmacro %}