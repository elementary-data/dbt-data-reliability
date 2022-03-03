{{
  config(
    materialized='table',
    post_hook="{{ monitors_query(2) }}"
  )
}}

-- depends_on: {{ ref('elementary_runs') }}
-- depends_on: {{ ref('final_tables_config') }}
-- depends_on: {{ ref('final_columns_config') }}
-- depends_on: {{ ref('final_should_backfill') }}


{{ empty_table([('full_table_name','str'),('column_name','str'),('metric_name','str'),('metric_value','int'),('timeframe_start','timestamp'),('timeframe_end','timestamp'),('timeframe_duration_hours','int')]) }}