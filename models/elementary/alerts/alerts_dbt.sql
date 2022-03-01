-- depends_on: {{ ref('dbt_run_results') }}
-- depends_on: {{ ref('dbt_tests') }}

select 1 as num