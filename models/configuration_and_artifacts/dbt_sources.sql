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
                ('source_name', 'string'),
                ('name', 'string'),
                ('identifier', 'string'),
                ('loaded_at_field', 'string'),
                ('freshness_warn_after', 'string'),
                ('freshness_error_after', 'string'),
                ('freshness_filter', 'string'),
                ('relation_name', 'string'),
                ('source_meta', 'string'),
                ('tags', 'string'),
                ('meta', 'string'),
                ('package_name', 'string'),
                ('original_path', 'string'),
                ('path', 'string'),
                ('source_description', 'string'),
                ('description', 'string')])
}}
