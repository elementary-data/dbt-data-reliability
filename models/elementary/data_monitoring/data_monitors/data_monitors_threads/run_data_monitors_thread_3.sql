{{
  config(
    materialized='table'
  )
}}

-- depends_on: {{ ref('elementary_runs') }}
-- depends_on: {{ ref('final_tables_config') }}
-- depends_on: {{ ref('final_columns_config') }}
-- depends_on: {{ ref('final_should_backfill') }}
-- depends_on: {{ ref('init_data_monitors_thread_3') }}

{{ monitors_query(3) }}