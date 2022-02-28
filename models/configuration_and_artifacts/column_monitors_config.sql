{{
  config(
    materialized = 'table',
    transient=false,
    unique_key = 'full_column_name',
    post_hook = "{% if execute %}{{ elementary.upload_columns_configuration() }}{% endif %}"
  )
}}

{{ empty_table([('full_column_name', 'string'), ('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string'), ('column_name', 'string'), ('column_monitors', 'string')]) }}