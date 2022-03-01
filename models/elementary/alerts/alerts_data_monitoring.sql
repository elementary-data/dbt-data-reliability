{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id'
  )
}}

with anomalies as (

    select * from {{ ref('anomaly_detection') }}

),

anomaly_alerts as (

     select
         id as alert_id,
         updated_at as detected_at,
         {{ elementary.full_name_to_db() }},
         {{ elementary.full_name_to_schema() }},
         {{ elementary.full_name_to_table() }},
         column_name,
         'anomaly_detection' as alert_type,
         metric_name as sub_type,
         description as alert_description
     from anomaly_detection

)

select * from anomaly_alerts
{% if is_incremental() %}
    {% set row_count = elementary.get_row_count(this) %}
    {% if row_count > 0 %}
        where detected_at > (select max(detected_at) from {{ this }})
    {%- endif %}
{%- endif %}
