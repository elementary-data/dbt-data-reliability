{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns',
    full_refresh=elementary.get_config_var('elementary_full_refresh'),
  )
}}

{% if is_incremental() %}
  {% set from_time %}
    coalesce((select {{ elementary.edr_timeadd('day', -2, 'max(start_time)') }} from {{ this }} ), 
              {{ elementary.edr_timeadd('day', -30, elementary.edr_current_timestamp()) }})
  {% endset %}
{% else %}
  {% set from_time = elementary.edr_timeadd('day', -30, elementary.edr_current_timestamp()) %}
{% endif %}

{{ elementary.get_query_history_query(from_time) }}
