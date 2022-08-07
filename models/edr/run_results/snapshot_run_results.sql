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
    run_results.generated_at,
    run_results.status,
    run_results.full_refresh,
    run_results.message,
    snapshots.database_name,
    snapshots.schema_name,
    snapshots.materialization,
    snapshots.tags,
    snapshots.path,
    snapshots.original_path,
    snapshots.owner,
    snapshots.alias
FROM dbt_run_results run_results
JOIN dbt_snapshots snapshots ON run_results.unique_id = snapshots.unique_id
