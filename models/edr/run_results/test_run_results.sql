{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with dbt_run_results as (
    select * from {{ ref('dbt_run_results') }}
),

dbt_tests as (
    select * from {{ ref('dbt_tests') }}
)

SELECT
    run_results.model_execution_id as test_execution_id,
    run_results.unique_id,
    run_results.generated_at,
    run_results.status,
    run_results.message,
    run_results.compiled_sql,
    tests.database_name,
    tests.schema_name,
    tests.test_column_name as column_name,
    tests.short_name as test_type,
    tests.name,
    tests.severity,
    tests.test_params,
    tests.tags,
    tests.model_tags,
    tests.model_owners,
    tests.alias,
    tests.meta,
    tests.path,
    tests.original_path
FROM dbt_run_results run_results
JOIN dbt_tests tests ON run_results.unique_id = tests.unique_id
