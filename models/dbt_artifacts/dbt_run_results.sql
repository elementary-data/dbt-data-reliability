{{
  config(
    materialized = 'incremental',
    unique_key = 'unique_id'
  )
}}

{{ empty_table([('unique_id', 'string'), ('database_name', 'string'), ('schema_name', 'string'), ('name', 'string'), ('alias','string'), ('status','string'), ('execution_time', 'float'), ('run_started_at', 'string')]) }}