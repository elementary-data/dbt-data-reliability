{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with elementary_test_results as (
    select * from {{ ref('elementary_test_results') }}
),

alerts_anomaly_detection as (
    select id as alert_id,
           data_issue_id,
           test_execution_id,
           test_unique_id,
           model_unique_id,
           detected_at,
           database_name,
           schema_name,
           table_name,
           column_name,
           test_type,
           test_sub_type,
           test_results_description,
           owners,
           tags,
           test_results_query,
           other,
           test_name,
           test_params,
           severity,
           status
        from elementary_test_results
        where lower(status) != 'pass' {%- if elementary.get_config_var('elementary_alerts_ignore_warn') -%} and lower(status) != 'warn' {%- endif -%} and test_type = 'anomaly_detection'
)

select * from alerts_anomaly_detection