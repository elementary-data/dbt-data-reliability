{% macro store_schema_test_results(flattened_test, schema_changes_sql) %}
  {% set elementary_test_results_rows = [] %}
  {% set schema_changes_rows = elementary.agate_to_dicts(elementary.run_query(schema_changes_sql)) %}
  {% for schema_changes_row in schema_changes_rows %}
    {% do elementary_test_results_rows.append(elementary.get_schema_changes_test_result_row(flattened_test, schema_changes_row, schema_changes_rows)) %}
  {% endfor %}
  {% do elementary.cache_elementary_test_results_rows(elementary_test_results_rows) %}
{% endmacro %}

{% macro get_schema_changes_test_result_row(flattened_test, schema_changes_row, schema_changes_rows) %}
  {% set elementary_test_row = elementary.get_dbt_test_result_row(flattened_test, schema_changes_rows) %}
  {% do elementary_test_row.update(schema_changes_row) %}
  {% do return(elementary_test_row) %}
{% endmacro %}
