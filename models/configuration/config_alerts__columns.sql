
with columns_config as (

    select *,
        {{ full_table_name() }}
     from {{ columns_configuration_table() }}

),


tables_alerts as (

    select * from {{ ref('config_alerts__tables') }}

),

all_sources as (

    {{ union_columns_from_monitored_schemas() }}

),

joined_columns_and_configuration as (

    select distinct
        upper(coalesce(alls.full_table_name, conf.full_table_name)) as full_table_name,
        upper(coalesce(alls.database_name, conf.database_name)) as database_name,
        upper(coalesce(alls.schema_name, conf.schema_name)) as schema_name,
        upper(coalesce(alls.table_name, conf.table_name)) as table_name,
        upper(coalesce(alls.column_name, conf.column_name)) as column_name,
        upper(coalesce(concat(alls.full_table_name, '.',alls.column_name),
            concat(conf.full_table_name, '.',conf.column_name)))
        as full_column_name,
        tables_alerts.alert_on_schema_changes as is_table_monitored,
        conf.alert_on_schema_changes as is_column_monitored,
        case
            when conf.alert_on_schema_changes = true then true
            when conf.alert_on_schema_changes = false then false
            else tables_alerts.alert_on_schema_changes
        end as alert_on_schema_changes

    from all_sources as alls
         full outer join columns_config as conf
            on (alls.full_table_name = conf.full_table_name
            and alls.column_name = conf.column_name)
         left join tables_alerts
            on (alls.full_table_name = tables_alerts.full_table_name)
)

select * from joined_columns_and_configuration
group by 1,2,3,4,5,6,7,8,9