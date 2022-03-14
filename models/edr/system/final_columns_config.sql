{{
  config(
    materialized = 'incremental',
    unique_key = 'config_id'
  )
}}

with columns_config as (

    select * from {{ elementary.get_source_path('edr_configuration', 'column_monitors_config') }}

),

tables_config as (

    select * from {{ ref('final_tables_config') }}
    where columns_monitored = true
    or (columns_monitored is null and table_monitored = true)

),

information_schema_columns as (

    select * from {{ ref('filtered_information_schema_columns') }}

),

config_explicit_columns as (

    select
        {{ dbt_utils.surrogate_key([
            'config.full_column_name', 'config.column_monitors',
        ]) }} as config_id,
        {{ elementary.full_table_name('config') }} as full_table_name,
        upper(config.database_name) as database_name,
        upper(config.schema_name) as schema_name,
        upper(config.table_name) as table_name,
        upper(config.column_name) as column_name,
        info_schema.data_type,
        column_monitors,
        {{ elementary.run_start_column() }} as config_loaded_at
    from
        information_schema_columns as info_schema join columns_config as config
        on (upper(info_schema.database_name) = upper(config.database_name)
            and upper(info_schema.schema_name) = upper(config.schema_name)
            and upper(info_schema.table_name) = upper(config.table_name)
            and upper(info_schema.column_name) = upper(config.column_name))

),

tables_with_no_explicit_columns as (

    select
        tab.full_table_name,
        tab.database_name,
        tab.schema_name,
        tab.table_name
    from tables_config as tab
        left join columns_config as col
        on (upper(tab.database_name) = upper(col.database_name)
            and upper(tab.schema_name) = upper(col.schema_name)
            and upper(tab.table_name) = upper(col.table_name))
    where col.table_name is null

),

config_no_explicit_columns as (

    select
        {{ dbt_utils.surrogate_key([
            'tab.full_table_name', 'info_schema.column_name'
        ]) }} as config_id,
        tab.full_table_name,
        upper(tab.database_name) as database_name,
        upper(tab.schema_name) as schema_name,
        upper(tab.table_name) as table_name,
        upper(info_schema.column_name) as column_name,
        info_schema.data_type,
        {{ elementary.null_string() }} as column_monitors,
        {{ elementary.run_start_column() }} as config_loaded_at
    from
        information_schema_columns as info_schema join tables_with_no_explicit_columns as tab
    on (upper(info_schema.database_name) = upper(tab.database_name)
        and upper(info_schema.schema_name) = upper(tab.schema_name)
        and upper(info_schema.table_name) = upper(tab.table_name))

),

config_existing_columns as (

    select * from config_explicit_columns
    union all
    select * from config_no_explicit_columns

),

final as (

    select
        config_id,
        full_table_name,
        database_name,
        schema_name,
        table_name,
        column_name,
        data_type,
        column_monitors,
        max(config_loaded_at) as config_loaded_at
    from config_existing_columns
    group by 1,2,3,4,5,6,7,8

)

select *
from final

