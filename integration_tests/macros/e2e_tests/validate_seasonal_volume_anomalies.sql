{% macro validate_seasonal_volume_anomalies_after_training() %}
    -- start by validating the non-seasonal anomalies
    {% set non_seasonal_test_name = "'elementary_volume_anomalies_users_per_day_weekly_seasonal_14__2__updated_at'" %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {% set query_to_get_test_result_rows_query %}
        select ALERT_RESULTS_QUERY
        from {{ alerts_relation }}
        where test_name = {{ non_seasonal_test_name }}
    {% endset %}
            {% do debug() %}
    {% set test_result_rows_query = elementary.result_column_to_list(query_to_get_test_result_rows_query) %}
    {% set anomalous_result_rows_description_query %}
        with test_result_rows as
            (
            {{ test_result_rows_query[0] }}
            ) select distinct anomaly_description
              from test_result_rows
              where is_anomalous
    {% endset %}
    {% set descriptions_for_errors = elementary.result_column_to_list(anomalous_result_rows_description_query) %}
    {{ assert_lists_contain_same_items(descriptions_for_errors, ['the last row_count value is 700.000. the average for this metric is 103.046.',
                                                                 'the last row_count value is 700.000. the average for this metric is 102.941.']) }}

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
