{% macro monitored_schemas() %}

    {% set monitor_query %}
        select {{ full_schema_name() }}
        from {{ configured_schemas_path() }}
        where alert_on_schema_changes = true
    {% endset %}

    {% set monitored = result_column_to_list(monitor_query) %}
    {% set monitored_tuple = strings_list_to_tuple(monitored) %}
    {{ return(monitored_tuple) }}

{% endmacro %}


{% macro monitored_tables() %}

    {% set tables_to_monitor_query %}
        select {{ full_table_name() }} as full_table_name
        from {{ configured_tables_path() }}
        where alert_on_schema_changes = true
    {% endset %}

    {% set monitored_tables = result_column_to_list(tables_to_monitor_query) %}
    {% set monitored_tables_tuple = strings_list_to_tuple(monitored_tables) %}
    {{ return(monitored_tables_tuple) }}

{% endmacro %}


{% macro excluded_tables() %}

    {% set tables_to_monitor_query %}
        select {{ full_table_name() }} as full_table_name
        from {{ configured_tables_path() }}
        where alert_on_schema_changes = false
    {% endset %}

    {% set monitored_tables = result_column_to_list(tables_to_monitor_query) %}
    {% set monitored_tables_tuple = strings_list_to_tuple(monitored_tables) %}
    {{ return(monitored_tables_tuple) }}

{% endmacro %}


{% macro monitored_columns() %}

    {% set monitor_query %}
        select upper(concat(database_name, '.', schema_name, '.', table_name, '.', column_name)) as full_column_name
        from {{ configured_columns_path() }}
        where alert_on_schema_changes = true
    {% endset %}

    {% set monitored = result_column_to_list(monitor_query) %}
    {% set monitored_tuple = strings_list_to_tuple(monitored) %}
    {{ return(monitored_tuple) }}

{% endmacro %}


{% macro excluded_columns() %}

    {% set monitor_query %}
        select upper(concat(database_name, '.', schema_name, '.', table_name, '.', column_name)) as full_column_name
        from {{ configured_columns_path() }}
        where alert_on_schema_changes = false
    {% endset %}

    {% set monitored = result_column_to_list(monitor_query) %}
    {% set monitored_tuple = strings_list_to_tuple(monitored) %}
    {{ return(monitored_tuple) }}

{% endmacro %}