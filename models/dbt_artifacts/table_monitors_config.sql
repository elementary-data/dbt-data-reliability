{{
  config(
    materialized = 'table',
    transient=false,
    unique_key = 'id'
  )
}}

{{ empty_table([('id', 'string'), ('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string'), ('timestamp_column', 'string'), ('bucket_duration_hours', 'int'), ('monitored', 'boolean'), ('monitors', 'string')]) }}
