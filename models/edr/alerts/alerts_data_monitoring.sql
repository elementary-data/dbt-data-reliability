{{
  config(
    materialized = 'view'
  )
}}

with elementary_test_results as (
    select * from {{ ref('elementary_test_results') }}
),

alerts_data_monitoring as (
    select *
        from elementary_test_results
        where lower(status) != 'pass' and alert_type = 'anomaly_detection'
)

select * from alerts_data_monitoring