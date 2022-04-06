with dbt_run_results as (
    select * from source('elementary_dbt_artifacts', 'dbt_run_results')
),

dbt_models as (
    select * from source('elementary_dbt_artifacts', 'dbt_models')
),

dbt_tests as (
    select * from source('elementary_dbt_artifacts', 'dbt_tests')
),

dbt_tests_with_models_metadata as (
    select
        dt.*,
        dm.tags as model_tags,
        dm.owner as model_owner,
        dm.name as model_name
    from dbt_tests dt left join dbt_models dm on dt.parent_model_unique_id = dm.unique_id
),

failed_dbt_tests as (
    select
        dr.model_execution_id as alert_id,
        dr.generated_at as detected_at,
        dt.database_name as database_name,
        dt.schema_name as schema_name,
        dt.model_name as table_name,
        dt.test_column_name as column_name,
        'dbt_test' as alert_type,
        dr.status as sub_type,
        dr.name as alert_description,
        dt.model_owner as owner,
        dt.model_tags as tags,
        dt.compiled_sql as alert_results_query,
        {{ elementary.null_string() }} as other
    from dbt_run_results dr left join dbt_tests_with_models_metadata dt on dr.unique_id = dt.unique_id
    where resource_type = 'test' and lower(status) != 'success' and dt.generated_at >= {{ run_started_at.strftime('%Y-%m-%d %H:%M:%S') }}
)

select * from failed_dbt_tests

