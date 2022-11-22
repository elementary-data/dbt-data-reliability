{% test schema_changes(model) %}
    -- depends_on: {{ ref('elementary_test_results') }}
    -- depends_on: {{ ref('schema_columns_snapshot') }}
    -- depends_on: {{ ref('filtered_information_schema_columns') }}

    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {% set test_name_in_graph = elementary.get_test_name_in_graph() %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ test_name_in_graph) }}
        {# creates temp relation for schema columns info #}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}

        {# get table configuration #}
        {%- set full_table_name = elementary.relation_to_full_name(model) %}
        {%- set model_relation = dbt.load_relation(model) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unable to find table `{}`".format(full_table_name)) }}
        {% endif %}

        {#- query current schema and write to temp test table -#}
        {{ elementary.edr_log('Started testing schema changes on:' ~ full_table_name) }}
        {%- set column_snapshot_query = elementary.get_columns_snapshot_query(full_table_name) %}
        {{ elementary.debug_log('column_snapshot_query - \n' ~ column_snapshot_query) }}
        {% set temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_name_in_graph, 'schema_changes', column_snapshot_query) %}

        {# query if there were schema changes since last execution #}
        {% set schema_changes_alert_query = elementary.get_columns_changes_query(full_table_name, temp_table_relation) %}
        {% set alerts_temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_name_in_graph, 'schema_changes_alerts', schema_changes_alert_query) %}
        {# return schema changes query as standard test query #}
        select * from {{ alerts_temp_table_relation }}

    {% else %}

        {# test must run an sql query #}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}