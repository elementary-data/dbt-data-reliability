{{
  config(
    materialized = 'view',
    bind=False
  )
}}

select unique_id,
       max_loaded_at
       snapshotted_at as detected_at,
       max_loaded_at_time_ago_in_s,
       status
from {{ ref('source_freshness_results') }}
where {{ not elementary.get_config_var('disable_source_freshness_alerts') }} and lower(status) != 'success'
