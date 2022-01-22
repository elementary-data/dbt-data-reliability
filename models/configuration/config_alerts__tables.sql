{% set configured_schemas = get_configured_schemas() %}

with schemas_config as (

    select * from {{ configured_schemas_path() }}

),


tables_config as (

    select *,
        {{ full_table_name()}}
    from {{ configured_tables_path() }}

),

filtered_information_schema_tables as (

    {{ query_different_schemas(get_tables_from_information_schema, configured_schemas) }}

),

joined_tables_and_configuration as (

    select
        upper(coalesce(info_schema.full_table_name, conf.full_table_name)) as full_table_name,
        upper(coalesce(info_schema.database_name, conf.database_name)) as database_name,
        upper(coalesce(info_schema.schema_name, conf.schema_name)) as schema_name,
        upper(coalesce(info_schema.table_name, conf.table_name)) as table_name,
        schemas_config.alert_on_schema_changes as is_schema_monitored,
        conf.alert_on_schema_changes as is_table_monitored,
        case
            when conf.alert_on_schema_changes = true then true
            when conf.alert_on_schema_changes = false then false
            else schemas_config.alert_on_schema_changes
        end as alert_on_schema_changes

    from filtered_information_schema_tables as info_schema
        full outer join tables_config as conf
            on (info_schema.full_table_name = conf.full_table_name)
        left join schemas_config
            on (info_schema.database_name = schemas_config.database_name
            and info_schema.schema_name = schemas_config.schema_name)
    group by 1,2,3,4,5,6,7

)

select * from joined_tables_and_configuration
