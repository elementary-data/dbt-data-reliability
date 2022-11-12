{% macro handle_tests_results() %}
    {% if execute and flags.WHICH in ['test', 'build'] %}
        {{ elementary.edr_log("Handling test results.") }}
        {% set test_results = elementary.get_cache("test_results") %}
        {% do elementary.enrich_test_results(test_results) %}
        {% set tables_cache = elementary.get_cache("tables") %}
        {% set test_metrics_tables = tables_cache.get("metrics") %}
        {% set test_columns_snapshot_tables = tables_cache.get("schema_snapshots") %}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {{ elementary.merge_data_monitoring_metrics(database_name, schema_name, test_metrics_tables) }}
        {{ elementary.merge_schema_columns_snapshot(database_name, schema_name, test_columns_snapshot_tables) }}
        {% if test_results %}
            {%- set elementary_test_results_relation = adapter.get_relation(database=database_name,
                                                                            schema=schema_name,
                                                                            identifier='elementary_test_results') -%}
            {%- do elementary.insert_rows(elementary_test_results_relation, test_results.values(), should_commit=True) -%}
        {% endif %}
    {% endif %}
    {{ elementary.debug_log("Handled test results successfully.") }}
    {{ return('') }}
{% endmacro %}

{% macro enrich_test_results(test_results) %}
{#
  {% for result in results %}
    {% for result_row in test_results.get(result.node.unique_id) %}
      {% do result_row.update({'status': result.status, 'failures': result.failures}) %}
      {% do result_row.setdefault('test_results_description', result.message) %}
    {% endfor %}
  {% endfor %}
#}
{% endmacro %}

{% macro get_cached_test_result_rows(flatten_test_node) %}
    {% set test_result_rows = elementary.get_cache("test_results").get(flatten_test_node.unique_id) %}
    {% if not test_result_rows %}
      {{ return([]) }}
    {% endif %}
    {{ return(elementary.agate_to_dicts(test_result_rows)) }}
{% endmacro %}

{% macro render_test_result_rows(test_result_rows) %}
  {% if (tojson(test_result_rows) | length) < elementary.get_column_size() %}
    {{ return(test_result_rows) }}
  {% endif %}
  {{ return(none) }}
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
