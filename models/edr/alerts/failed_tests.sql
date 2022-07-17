with all_test_alerts as (
     select * from {{ ref('elementary', 'alerts_schema_changes') }}
     union all
     select * from {{ ref('elementary', 'alerts_anomaly_detection') }}
     union all
     select * from {{ ref('elementary', 'alerts_dbt_tests') }}
)

select * from all_test_alerts
where {{ not elementary.get_config_var('disable_test_alerts') }}
