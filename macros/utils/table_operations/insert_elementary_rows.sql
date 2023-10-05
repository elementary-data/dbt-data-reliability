{% macro insert_elementary_rows(rows, identifier) %}
  {% do return(adapter.dispatch("insert_elementary_rows", "elementary")(rows, identifier)) %}
{% endmacro %}

{% macro default__insert_elementary_rows(rows, identifier) %}
  {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
  {% set elementary_test_results_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier=identifier) %}
  {% do elementary.insert_rows(elementary_test_results_relation, rows, should_commit=True) %}
{% endmacro %}