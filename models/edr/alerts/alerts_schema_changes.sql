{{
  config(
    materialized = 'view'
  )
}}


with elementary_test_results as (
    select * from {{ ref('elementary_test_results') }}
),

alerts_schema_changes as (
    select *
        from elementary_test_results
        where lower(status) != 'pass' and alert_type = 'schema_change'
)

select * from alerts_schema_changes