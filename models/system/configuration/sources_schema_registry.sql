with columns_monitoring_configuration as (

    select *
    from {{ get_columns_configuration() }}

),

source_tables_schema_configuration as (

    select
        {{ full_table_name() }},
        upper(column_name) as column_name,
        data_type
    from columns_monitoring_configuration

)

select * from source_tables_schema_configuration