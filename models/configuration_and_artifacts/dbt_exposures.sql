{{
  config(
    materialized = 'table',
    transient=false,
    unique_key = 'unique_id'
  )
}}

{{ empty_table([('unique_id', 'string'),
                ('name', 'string'),
                ('maturity', 'string'),
                ('type', 'string'),
                ('owner_email', 'string'),
                ('owner_name', 'string'),
                ('url', 'string'),
                ('depends_on_macros', 'string'),
                ('depends_on_nodes', 'string'),
                ('description', 'string'),
                ('tags', 'string'),
                ('meta', 'string'),
                ('package_name', 'string'),
                ('original_path', 'string'),
                ('path', 'string')])
}}

