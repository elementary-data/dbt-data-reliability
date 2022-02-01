with query_history as (

    select * from {{ ref('stg_query_history') }}

),

access_history as (

    select * from {{ ref('stg_access_history') }}

),

data_lineage as (

    select
        qh.query_id,
        qh.query_type,
        qh.query_text,
        qh.database_name,
        qh.schema_name,
        qh.role_name,
        qh.user_name,
        qh.rows_modified,
        qh.query_star_time,
        qh.query_end_time,
        ah.modified_table_name as target_table,
        ah.modified_table_type as target_table_type,
        ah.modified_columns as target_columns,
        ah.base_access_table_name as source_base_table,
        ah.base_access_table_type as source_base_table_type,
        ah.base_access_columns as source_base_columns,
        ah.direct_access_table_name as source_direct_table,
        ah.direct_access_table_type as source_direct_table_type,
        ah.direct_access_columns as source_direct_columns

    from query_history as qh
        join access_history as ah
        on (qh.query_id = ah.query_id)
    where qh.execution_status) = 'SUCCESS'
        and ah.modified_table_type = 'TABLE'
)

select * from data_lineage