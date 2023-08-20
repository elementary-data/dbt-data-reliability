{{ config(materialized="view", bind=False) }}

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
    run_results.thread_id,
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
    case
        when
            first_value(invocation_id) over (
                partition by
                    {{ elementary.edr_time_trunc("day", "run_results.generated_at") }}
                order by run_results.generated_at asc
                rows between unbounded preceding and unbounded following
            )
            = invocation_id
        then true
        else false
    end as is_the_first_invocation_of_the_day,
    case
        when
            last_value(invocation_id) over (
                partition by
                    {{ elementary.edr_time_trunc("day", "run_results.generated_at") }}
                order by run_results.generated_at asc
                rows between unbounded preceding and unbounded following
            )
            = invocation_id
        then true
        else false
    end as is_the_last_invocation_of_the_day

from dbt_run_results run_results
join dbt_models models on run_results.unique_id = models.unique_id
