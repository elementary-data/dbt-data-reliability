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
                ('name', 'string'),
                ('status', 'string'),
                ('resource_type', 'string'),
                ('execution_time', 'float'),
                ('execute_started_at', 'string'),
                ('execute_completed_at', 'string'),
                ('compile_started_at', 'string'),
                ('compile_completed_at', 'string'),
                ('rows_affected', 'int'),
                ('full_refresh', 'boolean')])
}}