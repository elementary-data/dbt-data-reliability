{{
  config(
    materialized = 'incremental',
    unique_key = 'run_id'
  )
}}

select
    {{ dbt_utils.hash(run_started_at.timestamp()) }} as run_id,
    {{ run_start_column() }} as run_started_at