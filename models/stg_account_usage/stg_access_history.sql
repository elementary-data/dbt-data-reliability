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
    from {{ source('snowflake_account_usage','access_history') }}
    where query_start_time > (current_date - {{ var('account_usage_days_back_limit') }})::timestamp
    qualify row_number() over (partition by query_id order by query_id) = 1

),

access_history as (

     select
             src.query_id,
             src.query_start_time,
             src.user_name,
             direct.value:"objectName"::varchar as direct_access_table_name,
             direct.value:"objectDomain"::varchar as direct_access_table_type,
             direct.value:"columns"::varchar as direct_access_columns,
             base.value:"objectName"::varchar as base_access_table_name,
             base.value:"objectDomain"::varchar as base_access_table_type,
             base.value:"columns"::varchar as base_access_columns,
             modified.value:"objectName"::varchar as modified_table_name,
             modified.value:"objectDomain"::varchar as modified_table_type,
             modified.value:"columns"::varchar as modified_columns

         from source as src,
            lateral flatten(input => src.direct_objects_accessed) as direct,
            lateral flatten(input => src.base_objects_accessed) as base,
            lateral flatten(input => src.base_objects_accessed) as modified
         where direct.value:"objectId" is not null
             and lower(base.value:"objectDomain") != 'stage'
             {% if is_incremental() %}
                 and query_start_time > (select max(query_start_time)  from {{ this }})
             {% endif %}
)

select * from access_history