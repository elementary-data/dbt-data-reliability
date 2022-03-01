{{
  config(
    materialized = 'table',
    transient=false,
    unique_key = 'full_table_name',
    post_hook = "{% if execute %}{{ elementary.upload_tables_configuration() }}{% endif %}"
  )
}}

{{ empty_table([('full_table_name', 'string'), ('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string'), ('timestamp_column', 'string'), ('bucket_duration_hours', 'int'), ('table_monitored', 'boolean'), ('table_monitors', 'string'), ('columns_monitored', 'boolean')]) }}
