
with schemas_config as (

    select * from {{ schemas_configuration_table() }}

),


tables_config as (

    select *,
        {{ full_table_name()}}
    from {{ tables_configuration_table() }}

),

all_sources as (

    {{ union_columns_from_monitored_schemas() }}

),

joined_tables_and_configuration as (

    select
        upper(coalesce(alls.full_table_name, conf.full_table_name)) as full_table_name,
        upper(coalesce(alls.database_name, conf.database_name)) as database_name,
        upper(coalesce(alls.schema_name, conf.schema_name)) as schema_name,
        upper(coalesce(alls.table_name, conf.table_name)) as table_name,
        schemas_config.alert_on_schema_changes as is_schema_monitored,
        conf.alert_on_schema_changes as is_table_monitored,
        case
            when conf.alert_on_schema_changes = true then true
            when conf.alert_on_schema_changes = false then false
            else schemas_config.alert_on_schema_changes
        end as alert_on_schema_changes

    from all_sources as alls
        full outer join tables_config as conf
            on (alls.full_table_name = conf.full_table_name)
        left join schemas_config
            on (alls.database_name = schemas_config.database_name
            and alls.schema_name = schemas_config.schema_name)


)

select * from joined_tables_and_configuration
group by 1,2,3,4,5,6,7