{{
  config(
    materialized = 'table',
    transient=false,
    unique_key = 'unique_id'
  )
}}

{{ empty_table([('unique_id', 'string'),
                ('database_name', 'string'),
                ('schema_name', 'string'),
                ('name', 'string'),
                ('short_name', 'string'),
                ('alias', 'string'),
                ('test_column_name', 'string'),
                ('severity', 'string'),
                ('warn_if', 'string'),
                ('error_if', 'string'),
                ('config_tags', 'string'),
                ('config_meta', 'string'),
                ('tags', 'string'),
                ('meta', 'string'),
                ('depends_on_macros', 'string'),
                ('depends_on_nodes', 'string'),
                ('description', 'string'),
                ('package_name', 'string'),
                ('original_path', 'string'),
                ('path', 'string')])
}}