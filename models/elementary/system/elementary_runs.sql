{{
  config(
    materialized = 'incremental',
    unique_key = 'run_id'
  )
}}

select
    '{{ invocation_id }}' as run_id,
    {{ elementary.run_start_column() }} as run_started_at
