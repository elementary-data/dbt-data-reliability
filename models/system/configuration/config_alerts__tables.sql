{% set schemas_monitoring_configuration %}
    {{ target.database ~"."~ target.schema ~"."~ var('elementary')['schemas_monitoring_configuration']}}
{% endset %}

{% set tables_monitoring_configuration %}
    {{ target.database ~"."~ target.schema ~"."~ var('elementary')['tables_monitoring_configuration']}}
{% endset %}



with schemas_config as (

    select * from {{ schemas_monitoring_configuration }}

),


tables_config as (

    select *,
        {{ full_table_name()}}
    from {{ tables_monitoring_configuration }}

),

all_sources as (

    {{ union_columns_from_monitored_schemas() }}

),

joined_tables_and_configuration as (

    select distinct
        all_sources.full_table_name,
        all_sources.database_name,
        all_sources.schema_name,
        all_sources.table_name,
        schemas_config.alert_on_schema_changes as is_schema_monitored,
        tables_config.alert_on_schema_changes as is_table_monitored,
        case
            when tables_config.alert_on_schema_changes = true then true
            when tables_config.alert_on_schema_changes = false then false
            else schemas_config.alert_on_schema_changes
        end as alert_on_schema_changes

    from all_sources
        left join schemas_config
        on (all_sources.database_name = schemas_config.db_name
        and all_sources.schema_name = schemas_config.schema_name)
        left join tables_config
        on (all_sources.full_table_name = tables_config.full_table_name)

)

select * from joined_tables_and_configuration