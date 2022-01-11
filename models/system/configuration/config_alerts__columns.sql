{% set columns_monitoring_configuration %}
    {{ target.database ~"."~ target.schema ~"."~ var('elementary')['columns_monitoring_configuration']}}
{% endset %}


with columns_config as (

    select *,
        {{ full_table_name() }}
     from {{ columns_monitoring_configuration }}

),


tables_alerts as (

    select * from {{ ref('config_alerts__tables') }}

),

all_sources as (

    {{ union_columns_from_monitored_schemas() }}

),

joined_columns_and_configuration as (

    select distinct
        all_sources.full_table_name,
        all_sources.database_name,
        all_sources.schema_name,
        all_sources.table_name,
        all_sources.column_name,
        tables_alerts.alert_on_schema_changes as is_table_monitored,
        columns_config.alert_on_schema_changes as is_column_monitored,
        case
            when columns_config.alert_on_schema_changes = true then true
            when columns_config.alert_on_schema_changes = false then false
            else tables_alerts.alert_on_schema_changes
        end as alert_on_schema_changes

    from all_sources
        left join tables_alerts
        on (all_sources.full_table_name = tables_alerts.full_table_name)
        left join columns_config
        on (all_sources.full_table_name = columns_config.full_table_name
        and all_sources.column_name = columns_config.column_name)

)

select * from joined_columns_and_configuration