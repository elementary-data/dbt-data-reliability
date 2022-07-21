{{
  config(
    materialized = 'view',
    bind=False
  )
}}

{% set anomaly_detection_relation = adapter.get_relation(this.database, this.schema, 'alerts_anomaly_detection') %}
// Backwards compatibility support for a renamed model.
{% set data_monitoring_relation = adapter.get_relation(this.database, this.schema, 'alerts_data_monitoring') %}

with failed_tests as (
     select * from {{ ref('elementary', 'alerts_schema_changes') }}
     union all
     select * from {{ ref('elementary', 'alerts_dbt_tests') }}

    {% if anomaly_detection_relation %}
        union all
        select * from {{ anomaly_detection_relation }}
    {% endif %}

    {% if data_monitoring_relation %}
        union all
        select * from {{ data_monitoring_relation }}
    {% endif %}
)

select * from failed_tests where {{ not elementary.get_config_var('disable_test_alerts') }}
