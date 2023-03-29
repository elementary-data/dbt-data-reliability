{% macro get_test_type(flattened_test) %}
    {% if flattened_test.test_namespace == "elementary" %}
        {%- set elementary_test_type = elementary.get_elementary_test_type(flattened_test) %}
    {% endif %}

    {%- if elementary_test_type and elementary_test_type != "python_test" %}
        {{ return(elementary_test_type) }}
    {%- else %}
        {{ return("dbt_test") }}
    {%- endif %}
{% endmacro %}

{% macro get_elementary_test_type(flattened_test) %}
    {%- set anomaly_detection_tests = [
        'volume_anomalies',
        'freshness_anomalies',
        'event_freshness_anomalies',
        'table_anomalies',
        'dimension_anomalies',
        'column_anomalies',
        'all_columns_anomalies'
    ] %}
    {%- set schema_changes_tests = [
        'schema_changes',
        'schema_changes_from_baseline',
        'json_schema'
    ] %}
    {%- set python_tests = [
        'python',
        'json_schema'
    ]   %}

  {% if flattened_test.test_namespace == "elementary" %}
    {% if flattened_test.short_name | lower in anomaly_detection_tests %}
      {% do return("anomaly_detection") %}
    {% elif flattened_test.short_name | lower in schema_changes_tests %}
      {% do return("schema_change") %}
    {% elif flattened_test.short_name | lower in python_tests %}
        {% do return("python_test") %}
    {% endif %}
  {% endif %}
{% endmacro %}