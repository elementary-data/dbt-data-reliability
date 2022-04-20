{% macro tests_validation() %}
    {% if execute %}
        {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00")~"'" %}
        -- no validation data which means table freshness and volume should alert
        {% if not elementary.table_exists_in_target('any_type_column_anomalies_validation') %}
            {{ validate_table_anomalies() }}
        {% else %}
            {{ validate_string_column_anomalies() }}
            {{ validate_numeric_column_anomalies() }}
        {% endif %}
    {% endif %}
    {{ return('') }}
{% endmacro %}


{% macro assert_value_in_list(value, list) %}
    {% set upper_value = value | upper %}
    {% if upper_value in list %}
        {% do elementary.edr_log("SUCCESS: " ~ upper_value  ~ " in list " ~ list) %}
        {{ return(0) }}
    {% else %}
        {% do elementary.edr_log("FAILED: " ~ upper_value ~ " not in list " ~ list) %}
        {{ return(1) }}
    {% endif %}
{% endmacro %}

{% macro assert_value_not_in_list(value, list) %}
    {% set upper_value = value | upper %}
    {% if upper_value not in list %}
        {% do elementary.edr_log("SUCCESS: " ~ upper_value  ~ " not in list " ~ list) %}
        {{ return(0) }}
    {% else %}
        {% do elementary.edr_log("FAILED: " ~ upper_value ~ " in list " ~ list) %}
        {{ return(1) }}
    {% endif %}
{% endmacro %}

{% macro assert_lists_contain_same_items(list1, list2) %}
    {% if list1 | length != list2 | length %}
        {% do elementary.edr_log("FAILED: " ~ list1 ~ " has different length than " ~ list2) %}
        {{ return(1) }}
    {% endif %}
    {% for item1 in list1 %}
        {% if item1 | lower not in list2 %}
            {% do elementary.edr_log("FAILED: " ~ item1 ~ " not in list " ~ list2) %}
            {{ return(1) }}
        {% endif %}
    {% endfor %}
    {% do elementary.edr_log("SUCCESS: " ~ list1  ~ " in list " ~ list2) %}
    {{ return(0) }}
{% endmacro %}

{% macro get_alerts_data_monitoring_relation() %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {%- set alerts_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier='alerts_data_monitoring') %}
    {{ return(alerts_relation) }}
{% endmacro %}

{% macro validate_table_anomalies() %}
    {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00")~"'" %}
    -- no validation data which means table freshness and volume should alert
    {% set alerts_relation = get_alerts_data_monitoring_relation() %}
    {% set freshness_validation_query %}
        select distinct table_name
            from {{ alerts_relation }}
            where sub_type = 'freshness' and detected_at >= {{ max_bucket_end }}
    {% endset %}
    {% set results = elementary.result_column_to_list(freshness_validation_query) %}
    {{ assert_value_not_in_list('u_any_type_column_anomalies', results) }}
    {{ assert_value_in_list('string_column_anomalies', results) }}
    {{ assert_value_in_list('numeric_column_anomalies', results) }}
    {{ assert_value_not_in_list('any_type_column_anomalies_training', results) }}
    {{ assert_value_in_list('string_column_anomalies_training', results) }}
    {% set alerts_relation = get_alerts_data_monitoring_relation() %}
    {% set row_count_validation_query %}
        select distinct table_name
        from {{ alerts_relation }}
            where sub_type = 'row_count' and detected_at >= {{ max_bucket_end }}
    {% endset %}
    {% set results = elementary.result_column_to_list(row_count_validation_query) %}
    {{ assert_value_in_list('u_any_type_column_anomalies', results) }}
    {{ assert_value_not_in_list('string_column_anomalies', results) }}
    {{ assert_value_not_in_list('any_type_column_anomalies_training', results) }}
    {{ assert_value_in_list('string_column_anomalies_training', results) }}
{% endmacro %}

{% macro validate_string_column_anomalies() %}
    {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set alerts_relation = get_alerts_data_monitoring_relation() %}
    {% set string_column_alerts %}
    select distinct column_name
    from {{ alerts_relation }}
        where lower(sub_type) = lower(column_name) and detected_at >= {{ max_bucket_end }}
                        and upper(table_name) = 'STRING_COLUMN_ANOMALIES'
    {% endset %}
    {% set results = elementary.result_column_to_list(string_column_alerts) %}
    {{ assert_lists_contain_same_items(results, ['min_length', 'max_length', 'average_length', 'missing_count',
                                                 'missing_percent']) }}
{% endmacro %}

{% macro validate_numeric_column_anomalies() %}
    {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set alerts_relation = get_alerts_data_monitoring_relation() %}
    {% set numeric_column_alerts %}
    select distinct column_name
    from {{ alerts_relation }}
        where lower(sub_type) = lower(column_name) and detected_at >= {{ max_bucket_end }}
                                and upper(table_name) = 'NUMERIC_COLUMN_ANOMALIES'
    {% endset %}
    {% set results = elementary.result_column_to_list(numeric_column_alerts) %}
    {{ assert_lists_contain_same_items(results, ['min', 'max', 'zero_count', 'zero_percent', 'average',
                                                 'standard_deviation', 'variance']) }}
{% endmacro %}