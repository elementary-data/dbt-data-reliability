{{
  config(
    materialized = 'table',
    transient=false
  )
}}

{{ empty_table([('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string'), ('timestamp_column', 'string'), ('bucket_duration_hours', 'int'), ('monitored', 'boolean'), ('monitors', 'string')]) }}
