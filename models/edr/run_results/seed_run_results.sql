{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with dbt_run_results as (
    select * from {{ ref('dbt_run_results') }}
),

dbt_seeds as (
    select * from {{ ref('dbt_seeds') }}
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
    seeds.database_name,
    seeds.schema_name,
    run_results.materialization,
    seeds.tags,
    seeds.package_name,
    seeds.path,
    seeds.original_path,
    seeds.owner,
    seeds.alias
FROM dbt_run_results run_results
JOIN dbt_seeds seeds ON run_results.unique_id = seeds.unique_id
