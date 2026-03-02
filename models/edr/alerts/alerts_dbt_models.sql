{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with error_models as (
  
    select  model_execution_id,
            unique_id,
            invocation_id,
            name,
            generated_at,
            status,
            full_refresh,
            message,
            execution_time,
            execute_started_at,
            execute_completed_at,
            compile_started_at,
            compile_completed_at,
            compiled_code,
            database_name,
            schema_name,
            materialization,
            tags,
            package_name,
            path,
            original_path,
            owner,
            alias 
    from {{ ref('model_run_results') }}
  
    union all
  
    select  model_execution_id,
            unique_id,
            invocation_id,
            name,
            generated_at,
            status,
            full_refresh,
            message,
            execution_time,
            execute_started_at,
            execute_completed_at,
            compile_started_at,
            compile_completed_at,
            compiled_code,
            database_name,
            schema_name,
            materialization,
            tags,
            package_name,
            path,
            original_path,
            owner,
            alias  
  from {{ ref('snapshot_run_results') }}
)


select model_execution_id as alert_id,
       unique_id,
       {{ elementary.edr_cast_as_timestamp("generated_at") }} as detected_at,
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
where {{ not elementary.get_config_var('disable_model_alerts') }} and lower(status) != 'success' {%- if elementary.get_config_var('disable_skipped_model_alerts') -%} and lower(status) != 'skipped' {%- endif -%}
