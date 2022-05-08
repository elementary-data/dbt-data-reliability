with test_run_results as (
    select * from {{ ref('test_run_results') }}
),

failed_dbt_tests as (
    select model_execution_id as alert_id,
           {{ elementary.null_string() }} as data_issue_id,
           model_execution_id as test_execution_id,
           generated_at as detected_at,
           database_name as database_name,
           schema_name as schema_name,
           parent_model_name as table_name,
           test_column_name as column_name,
           'dbt_test' as alert_type,
           {{ elementary.null_string() }} as sub_type,
           message as alert_description,
           model_owners as owners,
           model_tags as tags,
           compiled_sql as alert_results_query,
           {{ elementary.null_string() }} as other,
           short_name as test_name,
           test_params,
           severity,
           status
        from test_run_results
        where status != 'pass' and (test_namespace is null or lower(test_namespace) != 'elementary')
)

select * from failed_dbt_tests