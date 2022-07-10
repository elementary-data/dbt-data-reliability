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
           null as test_unique_id,
           unique_id as model_unique_id,
           generated_at as detected_at,
           database_name,
           schema_name,
           null as table_name,
           null as column_name,
           'model' as alert_type,
           null as sub_type,
           message as alert_description,
           owner as owners,
           tags,
           null as alert_results_query,
           null other,
           alias as name,
           null as test_params,
           status
    from model_run_results
    where lower(status) != 'success'
)

select * from model_alerts
{{ dbt_utils.group_by(18) }}
