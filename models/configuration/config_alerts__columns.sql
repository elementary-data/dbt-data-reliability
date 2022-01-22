{% set configured_schemas = get_configured_schemas() %}

with columns_config as (

    select *,
        {{ full_table_name() }}
     from {{ configured_columns_path() }}

),


tables_alerts as (

    select * from {{ ref('config_alerts__tables') }}

),

filtered_information_schema_columns as (

    {{ query_different_schemas(get_columns_from_information_schema, configured_schemas) }}

),

joined_columns_and_configuration as (

    select distinct
        upper(coalesce(info_schema.full_table_name, conf.full_table_name)) as full_table_name,
        upper(coalesce(info_schema.database_name, conf.database_name)) as database_name,
        upper(coalesce(info_schema.schema_name, conf.schema_name)) as schema_name,
        upper(coalesce(info_schema.table_name, conf.table_name)) as table_name,
        upper(coalesce(info_schema.column_name, conf.column_name)) as column_name,
        upper(coalesce(concat(info_schema.full_table_name, '.',info_schema.column_name),
            concat(conf.full_table_name, '.',conf.column_name)))
        as full_column_name,
        tables_alerts.alert_on_schema_changes as is_table_monitored,
        conf.alert_on_schema_changes as is_column_monitored,
        case
            when conf.alert_on_schema_changes = true then true
            when conf.alert_on_schema_changes = false then false
            else tables_alerts.alert_on_schema_changes
        end as alert_on_schema_changes

    from filtered_information_schema_columns as info_schema
         full outer join columns_config as conf
            on (info_schema.full_table_name = conf.full_table_name
            and info_schema.column_name = conf.column_name)
         left join tables_alerts
            on (info_schema.full_table_name = tables_alerts.full_table_name)
    group by 1,2,3,4,5,6,7,8,9
)

select * from joined_columns_and_configuration
