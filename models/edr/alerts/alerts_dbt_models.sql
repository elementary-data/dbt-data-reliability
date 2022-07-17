{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with model_run_results as (
    select * from {{ ref('model_run_results') }}
),

model_alerts as (
    select model_execution_id as alert_id,
           unique_id,
           generated_at as detected_at,
           database_name,
           materialization,
           path,
           schema_name,
           message,
           owner as owners,
           tags,
           alias,
           status,
           full_refresh
    from model_run_results
    where {{ not elementary.get_config_var('disable_model_alerts') }} and lower(status) != 'success'
)

select * from model_alerts
