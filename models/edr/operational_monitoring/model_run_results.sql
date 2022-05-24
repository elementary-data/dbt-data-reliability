{{
  config(
    materialized = 'view',
    enabled=var('dbt_run_results') and var('dbt_models')
  )
}}

with dbt_run_results as (
    select * from {{ ref('dbt_run_results') }}
),

models_metadata as (
    select * from {{ ref('dbt_models') }}
),

model_run_results as (
    select *
    from dbt_run_results where resource_type = 'model'
),

model_run_results_with_metadata as (
    select mr.*,
           alias,
           checksum,
           materialization,
           tags,
           meta,
           owner,
           database_name,
           schema_name,
           depends_on_macros,
           depends_on_nodes,
           description,
           package_name
    from model_run_results mr left join models_metadata mm on mr.unique_id = mm.unique_id
)

select * from model_run_results_with_metadata
