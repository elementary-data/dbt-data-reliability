{{
  config(
    materialized = 'table',
    transient=false
  )
}}

{{ empty_table([('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string'), ('column_name', 'string'), ('monitored', 'boolean'), ('monitors', 'string')]) }}