{{
  config(
    materialized = 'incremental',
    unique_key = 'invocation_id'
  )
}}

{{ elementary.empty_invocation_times() }}

{%- if not is_incremental() %}
    {{ elementary.insert_invocation_start() }}
{% endif %}