with source_tables_schema_configuration as (
    select
    upper(full_table_name) as full_table_name,
    upper(column_name) as column_name,
    type as data_type,
    -- placeholder for supporting is_nullable in configuration
    null as is_nullable
    from {{ var('elementary')['monitoring_configuration_table'] }}
    where monitored = true
)
select * from source_tables_schema_configuration