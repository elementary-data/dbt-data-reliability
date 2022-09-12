{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with elementary_test_results as (
    select * from {{ ref('elementary_test_results') }}
),

dbt_tests as (
    select * from {{ ref('dbt_tests') }}
),

first_time_test_occurred as (
    select 
        min(detected_at) as first_time_occurred,
        test_unique_id
    from elementary_test_results
    group by test_unique_id
)

SELECT
    test_results.id,
    test_results.data_issue_id,
    test_results.test_execution_id,
    test_results.test_unique_id,
    test_results.model_unique_id,
    test_results.detected_at,
    test_results.database_name,
    test_results.schema_name,
    test_results.table_name,
    test_results.column_name,
    test_results.test_type,
    test_results.test_sub_type,
    test_results.test_results_description,
    test_results.owners,
    test_results.tags,
    test_results.test_results_query,
    test_results.other,
    test_results.test_name,
    test_results.test_params,
    test_results.severity,
    test_results.status,
    first_occurred.first_time_occurred as test_first_seen_at
FROM elementary_test_results test_results
JOIN dbt_tests tests ON test_results.test_unique_id = tests.unique_id
LEFT JOIN first_time_test_occurred first_occurred ON test_results.test_unique_id = first_occurred.test_unique_id
