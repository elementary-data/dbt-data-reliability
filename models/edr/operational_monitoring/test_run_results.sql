{{
  config(
    materialized = 'view',
    enabled=var('dbt_run_results') and var('dbt_tests') and var('dbt_models') and var('dbt_sources')
  )
}}

with dbt_run_results as (
    select * from {{ ref('dbt_run_results') }}
),

tests_metadata as (
    select * from {{ ref('dbt_tests') }}
),

models_metadata as (
    select * from {{ ref('dbt_models') }}
),

sources_metadata as (
    select * from {{ ref('dbt_sources') }}
),

tests_metadata_with_model_name as (
    select tm.*,
           case when mm.name is not null then mm.name
                when sm.name is not null then sm.name
                else null
                end as model_name
        from tests_metadata tm left join models_metadata mm on tm.parent_model_unique_id = mm.unique_id
            left join sources_metadata sm on tm.parent_model_unique_id = sm.unique_id
),

test_run_results as (
    select *
    from dbt_run_results where resource_type = 'test'
),

test_run_results_with_metadata as (
    select tr.*,
           database_name,
           schema_name,
           short_name,
           test_column_name,
           severity,
           warn_if,
           error_if,
           test_params,
           test_namespace,
           tags as test_tags,
           model_tags,
           model_owners,
           meta as test_meta,
           depends_on_macros,
           depends_on_nodes,
           parent_model_unique_id,
           model_name as parent_model_name,
           description,
           package_name
    from test_run_results tr left join tests_metadata_with_model_name tm on tr.unique_id = tm.unique_id
)

select * from test_run_results_with_metadata