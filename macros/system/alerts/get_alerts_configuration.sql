
{% macro get_tables_for_alerts() %}

    {% set tables_for_alerts_query %}
        select full_table_name
        from {{ ref('config_alerts__tables') }}
        where alert_on_schema_changes = true
    {% endset %}

    {% set tables_for_alerts = column_to_list(tables_for_alerts_query) %}
    {% set tables_for_alerts_tuple = list_to_tuple(tables_for_alerts) %}
    {{ return(tables_for_alerts_tuple) }}

{% endmacro %}


{% macro get_columns_for_alerts() %}

    {% set columns_for_alerts_query %}
        select full_column_name
        from {{ ref('config_alerts__columns') }}
        where alert_on_schema_changes = true
    {% endset %}

    {% set columns_for_alerts = column_to_list(columns_for_alerts_query) %}
    {% set columns_for_alerts_tuple = list_to_tuple(columns_for_alerts) %}
    {{ return(columns_for_alerts_tuple) }}

{% endmacro %}

{% macro get_excluded_columns_for_alerts() %}

    {% set columns_for_alerts_query %}
        select full_column_name
        from {{ ref('config_alerts__columns') }}
        where alert_on_schema_changes = false
    {% endset %}

    {% set columns_for_alerts = column_to_list(columns_for_alerts_query) %}
    {% set columns_for_alerts_tuple = list_to_tuple(columns_for_alerts) %}
    {{ return(columns_for_alerts_tuple) }}

{% endmacro %}

{% macro get_excluded_tables_for_alerts() %}

    {% set tables_for_alerts_query %}
        select full_table_name
        from {{ ref('config_alerts__tables') }}
        where alert_on_schema_changes = false
    {% endset %}

    {% set tables_for_alerts = column_to_list(tables_for_alerts_query) %}
    {% set tables_for_alerts_tuple = list_to_tuple(tables_for_alerts) %}
    {{ return(tables_for_alerts_tuple) }}

{% endmacro %}


{% macro get_schemas_for_alerts() %}

    {% set schemas_for_alerts_query %}
        select
            {{ full_schema_name() }}
        from {{ schemas_configuration_table() }}
        where alert_on_schema_changes = true
    {% endset %}

    {% set schemas_for_alerts = column_to_list(schemas_for_alerts_query) %}
    {% set schemas_for_alerts_tuple = list_to_tuple(schemas_for_alerts) %}
    {{ return(schemas_for_alerts_tuple) }}

{% endmacro %}



