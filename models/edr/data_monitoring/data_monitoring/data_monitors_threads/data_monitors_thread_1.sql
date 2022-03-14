{{
  config(
    materialized='data_monitoring',
    thread_number='1'
  )
}}

-- depends_on: {{ ref('elementary_runs') }}
-- depends_on: {{ ref('final_tables_config') }}
-- depends_on: {{ ref('final_columns_config') }}