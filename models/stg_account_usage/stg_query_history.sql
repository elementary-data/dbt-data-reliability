-- TODO: add dbs filter
-- TODO: change  global time limit to var

{{
  config(
    materialized='incremental',
    unique_key='query_id'
  )
}}


with source as (

    select *
    from {{ source('snowflake_account_usage','query_history') }}
    qualify row_number() over (partition by query_id order by query_id) = 1

),

query_history AS (

    select
        query_id,
        database_name,
        schema_name,
        query_text,
        query_type,
        query_tag,
        role_name,
        user_name,
        rows_produced,
        rows_inserted,
        rows_updated,
        rows_inserted + rows_updated as rows_modified,
        rows_deleted,
        rows_unloaded,
        execution_status,
        error_code,
        error_message,
        warehouse_id,
        warehouse_name,
        end_time as query_end_time,
        start_time as query_start_time,
        total_elapsed_time as query_elapsed_time,
        bytes_spilled_to_local_storage    AS query_bytes_spillover_local,
        bytes_spilled_to_remote_storage   AS query_bytes_spillover_remote,
        bytes_scanned                     AS query_bytes_scanned
    from source
    where is_client_generated_statement = false
          and (lower(query_text) not ilike '%.query_history%')
          and query_type not in
          ('SHOW', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES', 'GET_FILES',
            'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE', 'ALTER_NETWORK_POLICY', 'ALTER_ACCOUNT',
            'ALTER_TABLE_DROP_CLUSTERING_KEY', 'ALTER_USER',  'CREATE_CUSTOMER_ACCOUNT', 'CREATE_NETWORK_POLICY',
            'CREATE_ROLE', 'CREATE_USER', 'DESCRIBE_QUERY', 'DROP_NETWORK_POLICY', 'DROP_ROLE', 'DROP_USER', 'LIST_FILES',
            'REMOVE_FILES', 'REVOKE')
          and start_time > (current_date - 14)::timestamp
          {% if is_incremental() %}
                query_start_time > (select max(query_start_time)  from {{ this }})
          {% endif %}

)

select *
from query_history
