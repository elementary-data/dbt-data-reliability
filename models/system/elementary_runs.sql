{{
  config(
    materialized = 'incremental',
    unique_key = 'run_id'
  )
}}

select
    {{ dbt_utils.hash(run_started_at) }} as run_id,
    {{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }} as run_started_at,
    {{ run_started_at.timestamp() * 1000000 }} as run_started_at_timestamp