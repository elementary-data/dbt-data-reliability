{% macro validate_seasonal_volume_anomalies_after_training() %}
    -- start by validating the non-seasonal anomalies
    {% set non_seasonal_test_name = "'elementary_volume_anomalies_users_per_day_weekly_seasonal_14__2__updated_at'" %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {% set query_to_get_test_result_rows_query %}
        select ALERT_RESULTS_QUERY
        from {{ alerts_relation }}
        where test_name = {{ non_seasonal_test_name }}
    {% endset %}
    {% set test_result_rows_query = elementary.result_column_to_list(query_to_get_test_result_rows_query) %}
    {{ assert_list_has_expected_length(test_result_rows_query, 1) }}
    {% set anomalous_result_rows_description_query %}
        with test_result_rows as
            (
            {{ test_result_rows_query[0] }}
            ) select distinct metric_value
              from test_result_rows
              where is_anomalous
    {% endset %}
    {% set numeric_metric_from_error = elementary.result_value(anomalous_result_rows_description_query) %}
    {{ assert_value(numeric_metric_from_error, 700.0) }}

    -- now the seasonal anomalies: should not have any rows.
    {% set seasonal_test_name = "'elementary_volume_anomalies_users_per_day_weekly_seasonal_14__day_of_week__2__updated_at'" %}
    {% set query_to_get_test_result_rows_query %}
        select ALERT_RESULTS_QUERY
        from {{ alerts_relation }}
        where test_name = {{ seasonal_test_name }}
    {% endset %}
    {% set seasonal_anomalies_after_training = elementary.run_query(query_to_get_test_result_rows_query) %}
    {{ assert_empty_table(seasonal_anomalies_after_training) }}
{% endmacro %}
