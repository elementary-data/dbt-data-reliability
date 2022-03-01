{{
  config(
    materialized='table',
  )
}}

-- depends_on: {{ ref('elementary_runs') }}
-- depends_on: {{ ref('edr_tables_config') }}
-- depends_on: {{ ref('edr_columns_config') }}
-- depends_on: {{ ref('temp_monitoring_metrics') }}

{{ monitors_query('1,2,3,4') }}

--TODO: rename to monitoring_metrics and run macro as post_hook
