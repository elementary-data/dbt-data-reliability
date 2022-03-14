{% macro anomaly_test(metric_name) %}

    {%- set anomaly_test_query = elementary.anomaly_test_query(metric_name) %}
    {{ anomaly_test_query }}

{% endmacro %}
