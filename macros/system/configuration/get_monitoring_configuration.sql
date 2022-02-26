-- TODO: remove old and unused

{% macro monitored_schemas() %}

    {% set monitor_query %}
        select {{ full_schema_name() }}
        from {{ get_configuration_path() }}
        where alert_on_schema_changes = true
              and table_name is null
              and column_name is null
    {% endset %}

    {% set monitored = result_column_to_list(monitor_query) %}
    {% set monitored_tuple = strings_list_to_tuple(monitored) %}
    {{ return(monitored_tuple) }}

{% endmacro %}


{% macro excluded_tables() %}

    {% set tables_to_monitor_query %}
        select {{ full_table_name() }} as full_table_name
        from {{ get_configuration_path() }}
        where alert_on_schema_changes = false
              and table_name is not null
              and column_name is null
    {% endset %}

    {% set monitored_tables = result_column_to_list(tables_to_monitor_query) %}
    {% set monitored_tables_tuple = strings_list_to_tuple(monitored_tables) %}
    {{ return(monitored_tables_tuple) }}

{% endmacro %}


{% macro old_monitored_columns() %}

    {% set monitor_query %}
        select {{ full_column_name() }} as full_column_name
        from {{ get_configuration_path() }}
        where alert_on_schema_changes = true
              and column_name is not null
    {% endset %}

    {% set monitored = result_column_to_list(monitor_query) %}
    {% set monitored_tuple = strings_list_to_tuple(monitored) %}
    {{ return(monitored_tuple) }}

{% endmacro %}


{% macro excluded_columns() %}

    {% set monitor_query %}
        select {{ full_column_name() }} as full_column_name
        from {{ get_configuration_path() }}
        where alert_on_schema_changes = false
              and column_name is not null
    {% endset %}

    {% set monitored = result_column_to_list(monitor_query) %}
    {% set monitored_tuple = strings_list_to_tuple(monitored) %}
    {{ return(monitored_tuple) }}

{% endmacro %}