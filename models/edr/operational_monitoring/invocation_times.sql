{{
  config(
    materialized = 'incremental',
    unique_key = 'invocation_id'
  )
}}

{{ elementary.empty_invocation_times() }}