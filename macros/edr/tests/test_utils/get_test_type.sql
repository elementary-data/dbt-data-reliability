{% macro get_test_type(flattened_test) %}
    {% if flattened_test.test_namespace == "elementary" %}
        {%- set elementary_test_type = elementary.get_elementary_test_type(
            flattened_test
        ) %}
    {% endif %}
    {% if elementary_test_type == "with_context" %}
        {% do return("dbt_test") %}
    {% endif %}
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
    {%- set with_context_tests = [
        "accepted_range_with_context",
        "expect_column_pair_values_a_to_be_greater_than_b_with_context",
        "expect_column_values_to_be_unique_with_context",
        "expect_column_values_to_match_regex_list_with_context",
        "expect_column_values_to_match_regex_with_context",
        "expect_column_values_to_not_be_null_with_context",
        "expect_compound_columns_to_be_unique_with_context",
        "expression_is_true_with_context",
        "not_empty_string_with_context",
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
