{{
  config(
    materialized = 'table',
    transient=false,
    unique_key = 'unique_id'
  )
}}

{{ empty_table([('unique_id', 'string'),
                ('alias', 'string'),
                ('checksum', 'string'),
                ('materialization', 'string'),
                ('config_tags', 'string'),
                ('config_meta', 'string'),
                ('tags', 'string'),
                ('meta', 'string'),
                ('database_name', 'string'),
                ('schema_name', 'string'),
                ('depends_on_macros', 'string'),
                ('depends_on_nodes', 'string'),
                ('description', 'string'),
                ('name', 'string'),
                ('package_name', 'string'),
                ('original_path', 'string'),
                ('path', 'string')])
}}