{{
  config(
    materialized = 'table',
    transient=false,
    unique_key = 'id',
    post_hook = "{% if execute %}{{ elementary_data_reliability.upload_columns_configuration() }}{% endif %}"
  )
}}

{{ empty_table([('id', 'string'), ('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string'), ('column_name', 'string'), ('monitored', 'boolean'), ('monitors', 'string')]) }}