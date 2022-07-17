{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    merge_update_columns = ['alert_id']
  )
}}

with all_test_alerts as (
     select * from {{ ref('elementary', 'alerts_schema_changes') }}
     union all
     select * from {{ ref('elementary', 'alerts_anomaly_detection') }}
     union all
     select * from {{ ref('elementary', 'alerts_dbt_tests') }}
)

select *, false as alert_sent from all_test_alerts
where {{ not elementary.get_config_var('disable_test_alerts') }}

{%- if is_incremental() %}
    and {{ get_new_alerts_where_clause(this) }}
{%- endif %}
