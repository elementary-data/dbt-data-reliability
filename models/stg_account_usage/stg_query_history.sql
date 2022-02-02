{{
  config(
    materialized='incremental',
    unique_key='query_id'
  )
}}

{%- set query_text_clean -%}
    replace(query_text, '"', '')
{%- endset -%}

with source as (

    select *
    from {{ source('snowflake_account_usage','query_history') }}
    where start_time > (current_date - {{ var('account_usage_days_back_limit') }})::timestamp
    qualify row_number() over (partition by query_id order by query_id) = 1

),

query_history as (

    select
        query_id,
        database_id,
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
        bytes_spilled_to_local_storage as query_bytes_spillover_local,
        bytes_spilled_to_remote_storage as query_bytes_spillover_remote,
        bytes_scanned as query_bytes_scanned

    from source
    where is_client_generated_statement = false
          and (lower(query_text) not ilike '%.query_history%')
          and query_type not in
          ('SHOW', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES', 'GET_FILES',
            'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE', 'ALTER_NETWORK_POLICY', 'ALTER_ACCOUNT',
            'ALTER_TABLE_DROP_CLUSTERING_KEY', 'ALTER_USER',  'CREATE_CUSTOMER_ACCOUNT', 'CREATE_NETWORK_POLICY',
            'CREATE_ROLE', 'CREATE_USER', 'DESCRIBE_QUERY', 'DROP_NETWORK_POLICY', 'DROP_ROLE', 'DROP_USER', 'LIST_FILES',
            'REMOVE_FILES', 'REVOKE')
          and ({{ like_any_string_from_list(query_text_clean , var('query_history_include_dbs'), right_string='.') }}
            or {{ where_in_list('database_name', var('query_history_include_dbs')) }})
          {% if var('query_history_exclude_dbs')|length > 0 %}
              and not ({{ like_any_string_from_list(query_text_clean, var('query_history_exclude_dbs'), right_string='.') }} and database_name is null)
          {% endif %}
          {% if is_incremental() %}
              and query_start_time > (select max(query_start_time)  from {{ this }})
          {% endif %}

)

select *
from query_history
