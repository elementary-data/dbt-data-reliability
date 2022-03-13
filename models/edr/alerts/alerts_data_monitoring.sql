{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id'
  )
}}

with anomalies as (

    select *,
        {{ elementary.anomaly_detection_description() }}
    from {{ ref('metrics_anomaly_score') }}
    where is_anomaly = true

),

anomaly_alerts as (

     select
         id as alert_id,
         updated_at as detected_at,
         {{ elementary.full_name_split('database_name') }},
         {{ elementary.full_name_split('schema_name') }},
         {{ elementary.full_name_split('table_name') }},
         column_name,
         'anomaly_detection' as alert_type,
         metric_name as sub_type,
         description as alert_description
     from anomalies

)

select * from anomaly_alerts
{% if is_incremental() %}
    {% set row_count = elementary.get_row_count(this) %}
    {% if row_count > 0 %}
        where detected_at > (select max(detected_at) from {{ this }})
    {%- endif %}
{%- endif %}
