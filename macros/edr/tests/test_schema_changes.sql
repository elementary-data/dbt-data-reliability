{% test schema_changes(model) %}
    -- depends_on: {{ ref('elementary_test_results') }}
    -- depends_on: {{ ref('schema_columns_snapshot') }}
    -- depends_on: {{ ref('filtered_information_schema_columns') }}

    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {% set test_name_in_graph = elementary.get_test_name_in_graph() %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ test_name_in_graph) }}
        {# creates temp relation for schema columns info #}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set schema_name = schema_name ~ elementary.get_config_var('tests_schema_name') %}
        {% set temp_schema_changes_table_name = elementary.table_name_with_suffix(test_name_in_graph, '__schema_changes') %}
        {{ elementary.debug_log('schema columns table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_schema_changes_table_name) }}
        {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_schema_changes_table_name,
                                                                                   type='table') -%}


        {# get table configuration #}
        {%- set full_table_name = elementary.relation_to_full_name(model) %}
        {%- set model_relation = dbt.load_relation(model) %}
        {% if not model_relation %}
            {{ elementary.test_log('monitored_table_not_found', full_table_name) }}
            {{ return(elementary.no_results_query()) }}
        {% endif %}

        {#- query current schema and write to temp test table -#}
        {{ elementary.edr_log('Started testing schema changes on:' ~ full_table_name) }}
        {%- set column_snapshot_query = elementary.get_columns_snapshot_query(full_table_name) %}
        {{ elementary.debug_log('column_snapshot_query - \n' ~ column_snapshot_query) }}
        {%- do elementary.create_or_replace(False, temp_table_relation, column_snapshot_query) %}

        {# query if there were schema changes since last execution #}
        {% set schema_changes_alert_query = elementary.get_columns_changes_query(full_table_name, temp_table_relation) %}
        {% set temp_alerts_table_name = elementary.table_name_with_suffix(test_name_in_graph, '__schema_changes_alerts') %}
        {{ elementary.debug_log('schema alerts table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_alerts_table_name) }}
        {% set alerts_temp_table_exists, alerts_temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_alerts_table_name,
                                                                                   type='table') -%}
        {% do elementary.create_or_replace(False, alerts_temp_table_relation, schema_changes_alert_query) %}
        {# return schema changes query as standard test query #}
        select * from {{ alerts_temp_table_relation }}

    {% else %}

        {# test must run an sql query #}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}