{% test schema_changes(model) %}
    {{ config(tags = ['elementary-tests']) }}
    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}
        {% set model_relation = elementary.get_model_relation_for_test(model, elementary.get_test_model()) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you override 'ref' or 'source')") }}
        {% endif %}

        {%- if elementary.is_ephemeral_model(model_relation) %}
            {{ exceptions.raise_compiler_error("The test is not supported for ephemeral models, model name: {}".format(model_relation.identifier)) }}
        {%- endif %}
        {% set test_table_name = elementary.get_elementary_test_table_name() %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ test_table_name) }}
        {# creates temp relation for schema columns info #}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}

        {#- get table configuration -#}
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}

        {#- query current schema and write to temp test table -#}
        {{ elementary.edr_log('Started testing schema changes on:' ~ full_table_name) }}
        {%- set column_snapshot_query = elementary.get_columns_snapshot_query(model_relation, full_table_name) %}
        {{ elementary.debug_log('column_snapshot_query - \n' ~ column_snapshot_query) }}

        {% set temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'schema_changes', column_snapshot_query) %}

        {# query if there were schema changes since last execution #}
        {% set schema_changes_alert_query = elementary.get_columns_changes_from_last_run_query(full_table_name, temp_table_relation) %}
        {{ elementary.debug_log('schema_changes_alert_query - \n' ~ schema_changes_alert_query) }}
        {% set alerts_temp_table_relation = elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'schema_changes_alerts', schema_changes_alert_query) %}

        {% set flattened_test = elementary.flatten_test(elementary.get_test_model()) %}
        {% set schema_changes_sql = 'select * from {}'.format(alerts_temp_table_relation) %}
        {% do elementary.store_schema_snapshot_tables_in_cache() %}
        {% do elementary.store_schema_test_results(flattened_test, schema_changes_sql) %}

        {# return schema changes query as standard test query #}
        {{ schema_changes_sql}}
        
    {% else %}

        {# test must run an sql query #}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}