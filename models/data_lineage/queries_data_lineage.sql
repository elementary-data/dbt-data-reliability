with query_and_access_history as (

    select * from {{ ref('query_and_access_history') }}

),

data_lineage as (

    select
        query_id,
        query_type,
        query_text,
        database_name,
        schema_name,
        role_name,
        user_name,
        rows_modified,
        query_end_time,
        query_elapsed_time,
        modified_table_name as target_table,
        array_agg(distinct direct_access_table_name) as source_tables

    from query_and_access_history
    where
        modified_table_name is not null
        and modified_table_name != direct_access_table_name
        and execution_status = 'SUCCESS'
    group by 1,2,3,4,5,6,7,8,9,10,11
    order by query_end_time

)

select * from data_lineage