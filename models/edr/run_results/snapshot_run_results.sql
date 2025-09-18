{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with dbt_run_results as (
    select * from {{ ref('dbt_run_results') }}
),

dbt_snapshots as (
    select * from {{ ref('dbt_snapshots') }}
)

SELECT
    run_results.model_execution_id,
    run_results.unique_id,
    run_results.invocation_id,
    run_results.query_id,
    run_results.name,
    run_results.generated_at,
    run_results.status,
    run_results.full_refresh,
    run_results.message,
    run_results.execution_time,
    run_results.execute_started_at,
    run_results.execute_completed_at,
    run_results.compile_started_at,
    run_results.compile_completed_at,
    run_results.compiled_code,
    run_results.adapter_response,
    run_results.thread_id,
    run_results.group_name,
    model_snapshots.database_name,
    model_snapshots.schema_name,
    coalesce(run_results.materialization, model_snapshots.materialization) as materialization,
    model_snapshots.tags,
    model_snapshots.package_name,
    model_snapshots.path,
    model_snapshots.original_path,
    model_snapshots.owner,
    model_snapshots.alias
FROM dbt_run_results run_results
JOIN dbt_snapshots model_snapshots ON run_results.unique_id = model_snapshots.unique_id
