{{
  config(
    materialized='table',
  )
}}

{{ empty_table([('full_table_name','str'),('column_name','str'),('metric_name','str'),('metric_value','int'),('timeframe_start','timestamp'),('timeframe_end','timestamp'),('timeframe_duration_hours','int')]) }}