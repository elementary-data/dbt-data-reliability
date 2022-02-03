with query_and_access_history as (

    select * from {{ ref('query_and_access_history') }}

),

data_lineage as (

    select
        query_id,
        query_type,
        query_text,
        database_id,
        database_name,
        schema_name,
        role_name,
        user_name,
        rows_modified,
        query_start_time,
        query_end_time,
        modified_table_name as target_table_name,
        modified_table_type as target_table_type,
        modified_columns as target_columns,
        direct_access_table_name as source_direct_table_name,
        direct_access_table_type as source_direct_table_type,
        direct_access_columns as source_direct_columns

    from query_and_access_history
    where
        target_table_name is not null
        and target_table_name != source_direct_table_name
        and execution_status = 'SUCCESS'

)

select * from data_lineage