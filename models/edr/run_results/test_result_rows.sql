{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    on_schema_change = 'append_new_columns',
    full_refresh=elementary.get_config_var('elementary_full_refresh')
  )
}}

{{ elementary.empty_table([
    ('test_execution_id','long_string'),
    ('result_row', 'long_string')
]) }}
