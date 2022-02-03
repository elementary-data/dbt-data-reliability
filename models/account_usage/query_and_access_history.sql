with query_history as (

    select * from {{ ref('stg_query_history') }}

),

access_history as (

    select * from {{ ref('stg_access_history') }}

),

query_and_access_history as (

    select
        qh.query_id,
        qh.database_id,
        qh.database_name,
        qh.schema_name,
        qh.query_text,
        qh.query_type,
        qh.query_tag,
        qh.role_name,
        qh.user_name,
        qh.rows_produced,
        qh.rows_inserted,
        qh.rows_updated,
        qh.rows_modified,
        qh.rows_deleted,
        qh.rows_unloaded,
        qh.execution_status,
        qh.error_code,
        qh.error_message,
        qh.warehouse_id,
        qh.warehouse_name,
        qh.query_end_time,
        qh.query_start_time,
        qh.query_elapsed_time,
        qh.query_bytes_spillover_local,
        qh.query_bytes_spillover_remote,
        qh.query_bytes_scanned,
        ah.modified_table_name,
        ah.modified_table_type,
        ah.modified_columns,
        ah.base_access_table_name,
        ah.base_access_table_type,
        ah.base_access_columns,
        ah.direct_access_table_name,
        ah.direct_access_table_type,
        ah.direct_access_columns

    from query_history as qh
        join access_history as ah
        on (qh.query_id = ah.query_id)

)

select * from query_and_access_history