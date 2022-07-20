{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with failed_tests as (
     select * from {{ ref('elementary', 'alerts_schema_changes') }}
     union all
     select * from {{ ref('elementary', 'alerts_data_monitoring') }}
     union all
     select * from {{ ref('elementary', 'alerts_dbt_tests') }}
)

select * from failed_tests where {{ not elementary.get_config_var('disable_test_alerts') }}
