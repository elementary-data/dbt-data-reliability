{% macro get_test_type(flattened_test) %}
    {% if flattened_test.test_namespace == "elementary" %}
        {%- set elementary_test_type = elementary.get_elementary_test_type(
            flattened_test
        ) %}
    {% endif %}
    {#
      with_context tests are reported as dbt_test. They behave like plain dbt tests -- they return
      failing rows and rely on the test materialization to collect them -- and the alerts models
      partition on this value exactly (alerts_dbt_tests filters test_type = 'dbt_test',
      alerts_anomaly_detection 'anomaly_detection', alerts_schema_changes 'schema_change'). Any
      other value would match none of them, so these tests would raise no alerts at all. The
      distinct "with_context" type exists so the test materialization can identify them
      explicitly; it is deliberately not surfaced as a test_type.
    #}
    {% if elementary_test_type == "with_context" %} {% do return("dbt_test") %} {% endif %}
    {% do return(elementary_test_type or "dbt_test") %}
{% endmacro %}

{% macro get_elementary_test_type(flattened_test) %}
    {%- set anomaly_detection_tests = [
        "volume_anomalies",
        "freshness_anomalies",
        "event_freshness_anomalies",
        "table_anomalies",
        "dimension_anomalies",
        "column_anomalies",
        "all_columns_anomalies",
    ] %}
    {%- set schema_changes_tests = [
        "schema_changes",
        "schema_changes_from_baseline",
        "json_schema",
    ] %}
    {#
      Unlike the two families above, these do not collect their own result rows -- they return the
      failing rows and depend on the test materialization to sample them. Naming them explicitly
      lets the materialization exempt them from its early return.
    #}
    {%- set with_context_tests = [
        "accepted_range_with_context",
        "expect_column_values_to_be_unique_with_context",
        "expect_column_values_to_match_regex_with_context",
        "expect_column_values_to_not_be_null_with_context",
        "not_null_with_context",
        "relationships_with_context",
    ] %}

    {% if flattened_test.short_name | lower in anomaly_detection_tests %}
        {% do return("anomaly_detection") %}
    {% elif flattened_test.short_name | lower in schema_changes_tests %}
        {% do return("schema_change") %}
    {% elif flattened_test.short_name | lower in with_context_tests %}
        {% do return("with_context") %}
    {% endif %}
    {% do return(none) %}
{% endmacro %}
