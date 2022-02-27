{{
  config(
    materialized = 'incremental',
    unique_key = 'model_execution_id'
  )
}}

{{ empty_table([('model_execution_id', 'string'),
                ('unique_id', 'string'),
                ('invocation_id', 'string'),
                ('run_started_at', 'string'),
                ('database_name', 'string'),
                ('schema_name', 'string'),
                ('name', 'string'),
                ('alias', 'string'),
                ('status', 'string'),
                ('resource_type', 'string'),
                ('execution_time', 'float'),
                ('execute_started_at', 'string'),
                ('execute_completed_at', 'string'),
                ('compile_started_at', 'string'),
                ('compile_completed_at', 'string'),
                ('rows_affected', 'int'),
                ('package_name', 'string'),
                ('original_path', 'string'),
                ('materialization', 'string'),
                ('test_column_name', 'string'),
                ('checksum', 'string'),
                ('config_tags', 'string'),
                ('config_meta', 'string'),
                ('tags', 'string'),
                ('meta', 'string'),
                ('depends_on_macros', 'string'),
                ('depends_on_nodes', 'string'),
                ('description', 'string'),
                ('full_refresh', 'boolean')])
}}

