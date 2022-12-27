{% macro handle_tests_results() %}
    {{ elementary.debug_log("Handling test results.") }}
    {% set cached_elementary_test_results = elementary.get_cache("elementary_test_results") %}
    {% set elementary_test_results = elementary.get_result_enriched_elementary_test_results(cached_elementary_test_results) %}
    {% set tables_cache = elementary.get_cache("tables") %}
    {% set test_metrics_tables = tables_cache.get("metrics") %}
    {% set test_columns_snapshot_tables = tables_cache.get("schema_snapshots") %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {{ elementary.merge_data_monitoring_metrics(database_name, schema_name, test_metrics_tables) }}
    {{ elementary.merge_schema_columns_snapshot(database_name, schema_name, test_columns_snapshot_tables) }}
    {% if elementary_test_results %}
      {% set elementary_test_results_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier='elementary_test_results') %}
      {% do elementary.insert_rows(elementary_test_results_relation, elementary_test_results, should_commit=True) %}
    {% endif %}
    {{ elementary.debug_log("Handled test results successfully.") }}
    {{ return('') }}
{% endmacro %}

{% macro get_result_enriched_elementary_test_results(cached_elementary_test_results) %}
  {% set elementary_test_results = [] %}

  {% for result in results | selectattr('node.resource_type', '==', 'test') %}
    {% set result = result.to_dict() %}
    {% set elementary_test_results_rows = cached_elementary_test_results.get(result.node.unique_id) %}

    {# Materializing the test failed and therefore was not added to the cache. #}
    {% if not elementary_test_results_rows %}
      {% set flattened_test = elementary.flatten_test(result.node) %}
      {% set elementary_test_results_rows = [elementary.get_dbt_test_result_row(flattened_test)] %}
    {% endif %}

    {% for elementary_test_results_row in elementary_test_results_rows %}
      {% set failures = elementary_test_results_row.get("failures", result.failures) %}
      {% set status = "pass" if failures == 0 else result.status %}
      {% do elementary_test_results_row.update({'status': status, 'failures': failures, 'invocation_id': invocation_id}) %}
      {% do elementary_test_results_row.setdefault('test_results_description', result.message) %}
      {% do elementary_test_results.append(elementary_test_results_row) %}
    {% endfor %}
  {% endfor %}

  {% do return(elementary_test_results) %}
{% endmacro %}

{% macro merge_data_monitoring_metrics(database_name, schema_name, test_metrics_tables) %}
    {%- if test_metrics_tables %}
        {%- set test_tables_union_query = elementary.union_metrics_query(test_metrics_tables) -%}
        {%- set target_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier='data_monitoring_metrics') -%}
        {%- set temp_relation = dbt.make_temp_relation(target_relation) -%}
        {%- if test_tables_union_query %}
            {{ elementary.debug_log('Running union query from test tables to ' ~ temp_relation.identifier) }}
            {%- do run_query(dbt.create_table_as(True, temp_relation, test_tables_union_query)) %}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
            {{ elementary.debug_log('Merging ' ~ temp_relation.identifier ~ ' to ' ~ target_relation.database ~ '.' ~ target_relation.schema ~ '.' ~ target_relation.identifier) }}
            {%- if target_relation and temp_relation and dest_columns %}
                {% set merge_sql = elementary.merge_sql(target_relation, temp_relation, 'id', dest_columns) %}
                {%- do run_query(merge_sql) %}
                {%- do adapter.commit() -%}
                {{ elementary.debug_log('Finished merging') }}
            {%- else %}
                {{ elementary.debug_log('Error: could not merge to table: ' ~ target_name) }}
            {%- endif %}
        {%- endif %}
    {%- endif %}
{% endmacro %}

{% macro merge_schema_columns_snapshot(database_name, schema_name, test_columns_snapshot_tables) %}
    {%- if test_columns_snapshot_tables %}
        {%- set test_tables_union_query = elementary.union_columns_snapshot_query(test_columns_snapshot_tables) -%}
        {%- set target_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier='schema_columns_snapshot') -%}
        {%- set temp_relation = dbt.make_temp_relation(target_relation) -%}
        {%- if test_tables_union_query %}
            {{ elementary.debug_log('Running union query from test tables to ' ~ temp_relation.identifier) }}
            {%- do run_query(dbt.create_table_as(True, temp_relation, test_tables_union_query)) %}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
            {{ elementary.debug_log('Merging ' ~ temp_relation.identifier ~ ' to ' ~ target_relation.database ~ '.' ~ target_relation.schema ~ '.' ~ target_relation.identifier) }}
            {%- if target_relation and temp_relation and dest_columns %}
                {% set merge_sql = elementary.merge_sql(target_relation, temp_relation, 'column_state_id', dest_columns) %}
                {%- do run_query(merge_sql) %}
                {%- do adapter.commit() -%}
                {{ elementary.debug_log('Finished merging') }}
            {%- else %}
                {{ elementary.debug_log('Error: could not merge to table: ' ~ target_name) }}
            {%- endif %}
        {%- endif %}
    {%- endif %}
{% endmacro %}
