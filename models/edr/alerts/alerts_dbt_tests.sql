{{
  config(
    materialized = 'view'
  )
}}

with elementary_test_results as (
    select * from {{ ref('elementary_test_results') }}
),

alerts_dbt_tests as (
    select *
        from elementary_test_results
        where lower(status) != 'pass' and alert_type = 'dbt_test'
)

select * from alerts_dbt_tests