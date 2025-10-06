{% test schema_changes_from_baseline(model, fail_on_added=False, enforce_types=False) %}
    {{ config(tags = ['elementary-tests']) }}
    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}
        {% set model_relation = elementary.get_model_relation_for_test(model, elementary.get_test_model()) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you override 'ref' or 'source)") }}
        {% endif %}
        {%- if elementary.is_ephemeral_model(model_relation) %}
            {{ exceptions.raise_compiler_error("The test is not supported for ephemeral models, model name: {}".format(model_relation.identifier)) }}
        {%- endif %}

        {% set test_table_name = elementary.get_elementary_test_table_name() %}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}

        {# Get baseline and store it in a table #}
        {% set baseline_columns = elementary.get_model_baseline_columns(model_relation, enforce_types=enforce_types) %}
        {% set baseline_table_relation = elementary.create_model_baseline_table(baseline_columns, database_name, tests_schema_name, test_table_name) %}

        {% set full_table_name = elementary.relation_to_full_name(model_relation) %}
        {% set changes_from_baseline_query = elementary.get_column_changes_from_baseline_query(model_relation, full_table_name, baseline_table_relation, include_added=fail_on_added) %}

        {% set flattened_test = elementary.flatten_test(elementary.get_test_model()) %}
        {% do elementary.store_schema_snapshot_tables_in_cache() %}
        {% do elementary.store_schema_test_results(flattened_test, changes_from_baseline_query) %}

        {{ changes_from_baseline_query }}
    {% else %}
        {# test must run an sql query #}
        {{ elementary.no_results_query() }}
    {% endif %}
{% endtest %}
