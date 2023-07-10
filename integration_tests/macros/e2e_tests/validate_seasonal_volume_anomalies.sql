{% macro validate_seasonal_volume_anomalies_after_training() %}
    {% set alerts_relation = ref('elementary_test_results') %}
    {% set query %}
        select test_alias, status
        from {{ alerts_relation }}
        where table_name = 'users_per_day_weekly_seasonal'
    {% endset %}
    {% set results = elementary.run_query(query) %}
    {{ assert_lists_contain_same_items(results, [
        ('volume_anomalies_no_seasonality_anomalies', 'fail'),
        ('volume_anomalies_with_seasonality_anomalies', 'pass'),
    ]) }}
{% endmacro %}
