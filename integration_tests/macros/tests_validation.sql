{% macro tests_validation() %}
    -- depends_on: {{ ref('alerts_data_monitoring') }}
    {% if execute and flags.WHICH == 'test' %}
        -- no validation data which means table freshness and volume should alert
        {% if not elementary.table_exists_in_target('any_type_column_anomalies_validation') %}
            {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00")~"'" %}
            {% set freshness_validation_query %}
                select distinct table_name
                    from {{ ref('alerts_data_monitoring') }}
                    where sub_type = 'freshness' and detected_at >= {{ max_bucket_end }}
            {% endset %}
            {% set results = elementary.result_column_to_list(freshness_validation_query) %}
            {{ assert_value_not_in_list('any_type_column_anomalies', results) }}
            {{ assert_value_in_list('string_column_anomalies', results) }}
            {{ assert_value_in_list('numeric_column_anomalies', results) }}
            {{ assert_value_not_in_list('any_type_column_anomalies_training', results) }}
            {{ assert_value_in_list('string_column_anomalies_training', results) }}
            {% set row_count_validation_query %}
                select distinct table_name
                from {{ ref('alerts_data_monitoring') }}
                    where sub_type = 'row_count' and detected_at >= {{ max_bucket_end }}
            {% endset %}
            {% set results = elementary.result_column_to_list(row_count_validation_query) %}
            {{ assert_value_in_list('any_type_column_anomalies', results) }}
            {{ assert_value_not_in_list('string_column_anomalies', results) }}
            {{ assert_value_not_in_list('any_type_column_anomalies_training', results) }}
            {{ assert_value_in_list('string_column_anomalies_training', results) }}
        {% else %}
        -- validation data exists which means column anomalies should alert
        {% endif %}
    {% endif %}
    {{ return('') }}
{% endmacro %}


{% macro assert_value_in_list(value, list) %}
    {% set upper_value = value | upper %}
    {% if upper_value in list %}
        {% do elementary.edr_log("SUCCESS: " ~ upper_value  ~ " in list " ~ list) %}
    {% else %}
        {% do elementary.edr_log("FAILED: " ~ upper_value ~ " not in list " ~ list) %}
    {% endif %}
{% endmacro %}

{% macro assert_value_not_in_list(value, list) %}
    {% set upper_value = value | upper %}
    {% if upper_value not in list %}
        {% do elementary.edr_log("SUCCESS: " ~ upper_value  ~ " not in list " ~ list) %}
    {% else %}
        {% do elementary.edr_log("FAILED: " ~ upper_value ~ " in list " ~ list) %}
    {% endif %}
{% endmacro %}