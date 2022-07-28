{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with error_models as (
    select * from {{ ref('model_run_results') }}
    union all
    select * from {{ ref('snapshot_run_results') }}
)


select model_execution_id as alert_id,
       unique_id,
       generated_at as detected_at,
       database_name,
       materialization,
       path,
       original_path,
       schema_name,
       message,
       owner as owners,
       tags,
       alias,
       status,
       full_refresh
from error_models
where {{ not elementary.get_config_var('disable_model_alerts') }} and lower(status) != 'success'
