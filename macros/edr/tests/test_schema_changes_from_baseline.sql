{% test schema_changes_from_baseline(model, fail_on_added=False, enforce_types=False) %}
    -- depends_on: {{ ref('schema_columns_snapshot') }}
    -- depends_on: {{ ref('filtered_information_schema_columns') }}

    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {% set test_name_in_graph = elementary.get_test_name_in_graph() %}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}

        {# Get baseline and store it in a table #}
        {% set baseline_columns = elementary.get_model_baseline_columns(model, enforce_types=enforce_types) %}
        {% set baseline_table_relation = elementary.create_model_baseline_table(baseline_columns, database_name, schema_name, test_name_in_graph) %}

        {% set full_table_name = elementary.relation_to_full_name(model) %}
        {% set changes_from_baseline_query = elementary.get_column_changes_from_baseline_query(full_table_name, baseline_table_relation, include_added=fail_on_added) %}
        {{ changes_from_baseline_query }}
    {% else %}
        {# test must run an sql query #}
        {{ elementary.no_results_query() }}
    {% endif %}
{% endtest %}
