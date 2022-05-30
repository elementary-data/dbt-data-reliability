{{
  config(
    materialized = 'incremental',
    unique_key = 'invocation_id'
  )
}}

{% if is_incremental() %}
    {{ elementary.empty_invocation_times() }}
{%- else %}
    select
        '{{ invocation_id }}' as invocation_id,
        {{ elementary.run_start_column() }} as invocation_started_at,
        {{ elementary.null_timestamp() }} as invocation_ended_at
{% endif %}