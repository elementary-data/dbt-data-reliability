
with columns_config as (

    select *,
        {{ full_table_name() }}
     from {{ get_columns_configuration() }}

),


tables_alerts as (

    select * from {{ ref('config_alerts__tables') }}

),

all_sources as (

    {{ union_columns_from_monitored_schemas() }}

),

joined_columns_and_configuration as (

    select distinct
        COALESCE(all.full_table_name, conf.full_table_name) as full_table_name,
        COALESCE(all.database_name, conf.database_name) as database_name,
        COALESCE(all.schema_name, conf.schema_name) as schema_name,
        COALESCE(all.table_name, conf.table_name) as table_name,
        COALESCE(all.column_name, conf.column_name) as column_name,
        COALESCE(concat(all.full_table_name, '.',all.column_name),
            concat(conf.full_table_name, '.',conf.column_name))
        as full_column_name,
        tables_alerts.alert_on_schema_changes as is_table_monitored,
        conf.alert_on_schema_changes as is_column_monitored,
        case
            when conf.alert_on_schema_changes = true then true
            when conf.alert_on_schema_changes = false then false
            else tables_alerts.alert_on_schema_changes
        end as alert_on_schema_changes

    from all_sources as all
         full outer join columns_config as conf
            on (all.full_table_name = conf.full_table_name
            and all.column_name = conf.column_name)
         left join tables_alerts
            on (all.full_table_name = tables_alerts.full_table_name)
)

select * from joined_columns_and_configuration