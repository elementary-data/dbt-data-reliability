{{
  config(
    materialized = 'incremental',
    transient=False,
    unique_key = 'model_execution_id',
    on_schema_change = 'append_new_columns'
  )
}}

{{ elementary.get_dbt_run_results_empty_table_query() }}