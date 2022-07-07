with dbt_run_results as (
    select * from {{ ref('dbt_run_results') }}
),

dbt_models as (
    select * from {{ ref('dbt_models') }}
)

SELECT
    run_results.unique_id,
    run_results.generated_at,
    run_results.status,
    run_results.full_refresh,
    models.materialization,
    models.tags,
    models.path,
    models.owner,
    models.alias
FROM dbt_run_results run_results
JOIN dbt_models models ON run_results.unique_id = models.unique_id
