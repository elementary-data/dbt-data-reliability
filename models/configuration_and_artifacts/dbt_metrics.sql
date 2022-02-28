{{
  config(
    materialized = 'table',
    transient=false,
    unique_key = 'unique_id'
  )
}}

{{ empty_table([('unique_id', 'string'),
                ('name', 'string'),
                ('label', 'string'),
                ('model', 'string'),
                ('type', 'string'),
                ('sql', 'string'),
                ('timestamp', 'string'),
                ('filters', 'string'),
                ('time_grains', 'string'),
                ('dimensions', 'string'),
                ('depends_on_macros', 'string'),
                ('depends_on_nodes', 'string'),
                ('description', 'string'),
                ('tags', 'string'),
                ('meta', 'string'),
                ('package_name', 'string'),
                ('original_path', 'string'),
                ('path', 'string')])
}}


