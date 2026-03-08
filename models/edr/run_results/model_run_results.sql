{{ config(materialized="view", bind=False) }}

{%- set day_partition = elementary.edr_time_trunc("day", "run_results.generated_at") -%}
{%- set day_window = (
    "over (partition by "
    ~ day_partition
    ~ " order by run_results.generated_at asc rows between unbounded preceding and unbounded following)"
) -%}
{%- set first_inv_cond = (
    "first_value(invocation_id) " ~ day_window ~ " = invocation_id"
) -%}
{%- set last_inv_cond = (
    "last_value(invocation_id) " ~ day_window ~ " = invocation_id"
) -%}

with
    dbt_run_results as (select * from {{ ref("dbt_run_results") }}),

    dbt_models as (select * from {{ ref("dbt_models") }})

select
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
    models.database_name,
    models.schema_name,
    coalesce(run_results.materialization, models.materialization) as materialization,
    models.tags,
    models.package_name,
    models.path,
    models.original_path,
    models.owner,
    models.alias,
    row_number() over (
        partition by run_results.unique_id order by run_results.generated_at desc
    ) as model_invocation_reverse_index,
    {{ elementary.edr_condition_as_boolean(first_inv_cond) }}
    as is_the_first_invocation_of_the_day,
    {{ elementary.edr_condition_as_boolean(last_inv_cond) }}
    as is_the_last_invocation_of_the_day

from dbt_run_results run_results
join dbt_models models on run_results.unique_id = models.unique_id
