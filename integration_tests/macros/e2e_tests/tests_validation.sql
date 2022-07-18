{% macro tests_validation() %}
    {% if execute %}
        {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
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


{% macro assert_value_in_list(value, list, context='') %}
    {% set upper_value = value | upper %}
    {% set lower_value = value | lower %}
    {% if upper_value in list or lower_value in list %}
        {% do elementary.edr_log(context ~ " SUCCESS: " ~ upper_value  ~ " in list " ~ list) %}
        {{ return(0) }}
    {% else %}
        {% do elementary.edr_log(context ~ " FAILED: " ~ upper_value ~ " not in list " ~ list) %}
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

{% macro assert_list1_in_list2(list1, list2, context = '') %}
    {% set lower_list2 = list2 | lower %}
    {% if not list1 or not list2 %}
        {% do elementary.edr_log(context ~ " FAILED: list1 is empty or list2 is empty") %}
        {{ return(1) }}
    {% endif %}
    {% for item1 in list1 %}
        {% if item1 | lower not in lower_list2 %}
            {% do elementary.edr_log(context ~ " FAILED: " ~ item1 ~ " not in list " ~ list2) %}
            {{ return(1) }}
        {% endif %}
    {% endfor %}
    {% do elementary.edr_log(context ~ " SUCCESS: " ~ list1  ~ " in list " ~ list2) %}
    {{ return(0) }}
{% endmacro %}


{% macro get_alerts_table_relation(table_name) %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {%- set alerts_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier=table_name) %}
    {{ return(alerts_relation) }}
{% endmacro %}

{% macro validate_table_anomalies() %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    -- no validation data which means table freshness and volume should alert
    {% set alerts_relation = get_alerts_table_relation('alerts_anomaly_detection') %}
    {% set freshness_validation_query %}
        select distinct table_name
            from {{ alerts_relation }}
            where sub_type = 'freshness' and detected_at >= {{ max_bucket_end }}
    {% endset %}
    {% set results = elementary.result_column_to_list(freshness_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['string_column_anomalies',
                                                 'numeric_column_anomalies',
                                                 'string_column_anomalies_training']) }}
    {% set row_count_validation_query %}
        select distinct table_name
        from {{ alerts_relation }}
            where sub_type = 'row_count' and detected_at >= {{ max_bucket_end }}
    {% endset %}
    {% set results = elementary.result_column_to_list(row_count_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['any_type_column_anomalies',
                                                 'numeric_column_anomalies',
                                                 'string_column_anomalies_training']) }}

{% endmacro %}

{% macro validate_string_column_anomalies() %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set alerts_relation = get_alerts_table_relation('alerts_anomaly_detection') %}
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
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set alerts_relation = get_alerts_table_relation('alerts_anomaly_detection') %}
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


{% macro validate_any_type_column_anomalies() %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set alerts_relation = get_alerts_table_relation('alerts_anomaly_detection') %}
    {% set any_type_column_alerts %}
        select column_name, sub_type
        from {{ alerts_relation }}
            where detected_at >= {{ max_bucket_end }} and upper(table_name) = 'ANY_TYPE_COLUMN_ANOMALIES'
                  and column_name is not NULL
            group by 1,2
    {% endset %}
    {% set alert_rows = run_query(any_type_column_alerts) %}
    {% set indexed_columns = {} %}
    {% for row in alert_rows %}
        {% set column_name = row[0] %}
        {% set alert = row[1] %}
        {% if column_name in indexed_columns %}
            {% do indexed_columns[column_name].append(alert) %}
        {% else %}
            {% do indexed_columns.update({column_name: [alert]}) %}
        {% endif %}
    {% endfor %}
    {% set results = [] %}
    {% for column, column_alerts in indexed_columns.items() %}
        {% for alert in column_alerts %}
            {% if alert | lower in column | lower %}
                {% do results.append(column) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {{ assert_lists_contain_same_items(results, ['null_count_str',
                                                 'null_percent_str',
                                                 'null_count_float',
                                                 'null_percent_float',
                                                 'null_count_int',
                                                 'null_percent_int',
                                                 'null_count_bool',
                                                 'null_percent_bool']) }}
{% endmacro %}

{% macro validate_no_timestamp_anomalies() %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set alerts_relation = get_alerts_table_relation('alerts_anomaly_detection') %}

    {# Validating row count for no timestamp table anomaly #}
    {% set no_timestamp_row_count_validation_query %}
        select distinct table_name
        from {{ alerts_relation }}
        where sub_type = 'row_count'
        and upper(table_name) = 'NO_TIMESTAMP_ANOMALIES'
        and detected_at >= {{ max_bucket_end }}
    {% endset %}
    {% set results = elementary.result_column_to_list(no_timestamp_row_count_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['no_timestamp_anomalies']) }}

    {# Validating any column anomaly with no timestamp #}
    {% set no_timestamp_column_validation_alerts %}
        select column_name, sub_type
        from {{ alerts_relation }}
            where detected_at >= {{ max_bucket_end }} and upper(table_name) = 'NO_TIMESTAMP_ANOMALIES'
                  and column_name is not NULL
            group by 1,2
    {% endset %}
    {% set alert_rows = run_query(no_timestamp_column_validation_alerts) %}
    {% set indexed_columns = {} %}
    {% for row in alert_rows %}
        {% set column_name = row[0] %}
        {% set alert = row[1] %}
        {% if column_name in indexed_columns %}
            {% do indexed_columns[column_name].append(alert) %}
        {% else %}
            {% do indexed_columns.update({column_name: [alert]}) %}
        {% endif %}
    {% endfor %}
    {% set results = [] %}
    {% for column, column_alerts in indexed_columns.items() %}
        {% for alert in column_alerts %}
            {% if alert | lower in column | lower %}
                {% do results.append(column) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {{ assert_lists_contain_same_items(results, ['null_count_str']) }}
{% endmacro %}

{% macro validate_error_test() %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set alerts_relation = get_alerts_table_relation('alerts_dbt_tests') %}

    {# Validating alert for error test was created #}
    {% set error_test_validation_query %}
        select distinct status
        from {{ alerts_relation }}
        where status = 'error'
        and detected_at >= {{ max_bucket_end }}
    {% endset %}
    {% set results = elementary.result_column_to_list(error_test_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['error']) }}
{% endmacro %}

{% macro validate_schema_changes() %}
    {% set expected_changes = {'red_cards': 'column_added',
                               'group_a':   'column_removed',
                               'goals':   'type_changed',
                               'key_crosses': 'column_added',
                               'offsides': 'column_removed'} %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set alerts_relation = get_alerts_table_relation('alerts_schema_changes') %}
    {% set schema_changes_alerts %}
    select column_name, sub_type
    from {{ alerts_relation }}
        where detected_at >= {{ max_bucket_end }} and column_name is not NULL
    group by 1,2
    {% endset %}
    {% set alert_rows = run_query(schema_changes_alerts) %}
    {% set found_schema_changes = {} %}
    {% for row in alert_rows %}
        {% set column_name = row[0] | lower %}
        {% set alert = row[1] | lower %}
        {% if column_name not in expected_changes %}
            {% do elementary.edr_log("FAILED: could not find expected alert for " ~ column_name ~ ", " ~ alert) %}
            {{ return(1) }}
        {% endif %}
        {% if expected_changes[column_name] != alert %}
            {% do elementary.edr_log("FAILED: for column " ~ column_name ~ " expected alert type " ~ expected_changes[column_name] ~ " but got " ~ alert) %}
            {{ return(1) }}
        {% endif %}
        {% do found_schema_changes.update({column_name: alert}) %}
    {% endfor %}
    {% if found_schema_changes %}
        {%- set missing_changes = [] %}
        {%- for expected_change in expected_changes %}
            {%- if expected_change | lower not in found_schema_changes %}
                {% do elementary.edr_log("FAILED: for column " ~ expected_change ~ " expected alert " ~ expected_changes[expected_change] ~ " but alert is missing") %}
                {%- do missing_changes.append(expected_change) -%}
            {%- endif %}
        {%- endfor %}
        {%- if missing_changes | length == 0 %}
            {% do elementary.edr_log("SUCCESS: all expected schema changes were found - " ~ found_schema_changes) %}
            {{ return(0) }}
        {%- endif %}
    {% endif %}
    {{ return(0) }}
{% endmacro %}

{% macro validate_regular_tests() %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set alerts_relation = get_alerts_table_relation('alerts_dbt_tests') %}
    {% set dbt_test_alerts %}
        select table_name, column_name, test_name
        from {{ alerts_relation }}
            where detected_at >= {{ max_bucket_end }}
        group by 1,2, 3
    {% endset %}
    {% set alert_rows = run_query(dbt_test_alerts) %}
    {% set found_tables = [] %}
    {% set found_columns = [] %}
    {% set found_tests = [] %}
    {% for row in alert_rows %}
        {%- if row[0] -%}
            {% do found_tables.append(row[0]) %}
        {%- endif -%}
        {%- if row[1] -%}
            {% do found_columns.append(row[1]) %}
        {%- endif -%}
        {%- if row[2] -%}
            {% do found_tests.append(row[2]) %}
        {%- endif -%}
    {% endfor %}
    {{ assert_lists_contain_same_items(found_tables, ['string_column_anomalies', 'numeric_column_anomalies', 'any_type_column_anomalies', 'any_type_column_anomalies_validation', 'numeric_column_anomalies_training']) }}
    {{ assert_lists_contain_same_items(found_columns, ['min_length', 'null_count_int']) }}
    {{ assert_lists_contain_same_items(found_tests, ['relationships', 'singular_test_with_no_ref', 'singular_test_with_one_ref', 'singular_test_with_two_refs', 'singular_test_with_source_ref', 'generic_test_on_model', 'generic_test_on_column']) }}

{% endmacro %}

{% macro get_artifacts_table_relation(table_name) %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {%- set artifacts_relation = adapter.get_relation(database=database_name,
                                                      schema=schema_name,
                                                      identifier=table_name) %}
    {{ return(artifacts_relation) }}
{% endmacro %}

{% macro validate_dbt_artifacts() %}
    {% set dbt_models_relation = get_artifacts_table_relation('dbt_models') %}
    {% set dbt_models_query %}
        select distinct name from {{ dbt_models_relation }}
    {% endset %}
    {% set models = elementary.result_column_to_list(dbt_models_query) %}
    {{ assert_value_in_list('any_type_column_anomalies', models, context='dbt_models') }}
    {{ assert_value_in_list('numeric_column_anomalies', models, context='dbt_models') }}
    {{ assert_value_in_list('string_column_anomalies', models, context='dbt_models') }}

    {% set dbt_sources_relation = get_artifacts_table_relation('dbt_sources') %}
    {% set dbt_sources_query %}
        select distinct name from {{ dbt_sources_relation }}
    {% endset %}
    {% set sources = elementary.result_column_to_list(dbt_sources_query) %}
    {{ assert_value_in_list('any_type_column_anomalies_training', sources, context='dbt_sources') }}
    {{ assert_value_in_list('string_column_anomalies_training', sources, context='dbt_sources') }}
    {{ assert_value_in_list('any_type_column_anomalies_validation', sources, context='dbt_sources') }}

    {% set dbt_tests_relation = get_artifacts_table_relation('dbt_tests') %}
    {% set dbt_tests_query %}
        select distinct name from {{ dbt_tests_relation }}
    {% endset %}
    {% set tests = elementary.result_column_to_list(dbt_tests_query) %}

    {% set dbt_run_results = get_artifacts_table_relation('dbt_run_results') %}
    {% set dbt_run_results_query %}
        select distinct name from {{ dbt_run_results }} where resource_type in ('model', 'test')
    {% endset %}
    {% set run_results = elementary.result_column_to_list(dbt_run_results_query) %}
    {% set all_executable_nodes = [] %}
    {% do all_executable_nodes.extend(models) %}
    {% do all_executable_nodes.extend(tests) %}
    {{ assert_list1_in_list2(run_results, all_executable_nodes, context='dbt_run_results') }}
{% endmacro %}