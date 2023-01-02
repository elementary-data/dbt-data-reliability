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


{% macro validate_table_anomalies() %}
    {%- set max_bucket_end = elementary.quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    -- no validation data which means table freshness and volume should alert
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
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

{% macro validate_dimension_anomalies() %}
    {%- set max_bucket_end = elementary.quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
    {% set dimension_validation_query %}
        select *
            from {{ alerts_relation }}
            where sub_type = 'dimension' and detected_at >= {{ max_bucket_end }} and status = 'fail'
    {% endset %}
    {% set results = elementary.agate_to_dicts(run_query(dimension_validation_query)) %}
    {% set dimensions_with_problems = [] %}
    
    {% for result in results %}
        {% set test_params = fromjson(result.get('test_params', '{}')) %}
        {% set where_expression = test_params.get('where_expression') %}
        {% set dimensions = test_params.get('dimensions') %}
        {% if where_expression %}
            {% do dimensions_with_problems.append[dimensions] %}
        {% endif %}
    {% endfor %}

    {% if results | length != 2 %}
        {% do elementary.edr_log('FAILED: dimension anomalies tests failed because it has too many fail/error tests') %}
        {{ return(1) }}
    {% elif dimensions_with_problems %}
        {% do elementary.edr_log('FAILED: dimension anomalies tests failed on the dimensions - ' ~ dimensions_with_problems) %}
        {{ return(1) }}
    {% else %}
        {% do elementary.edr_log('SUCCESS: dimension anomalies tests succeeded') %}
        {{ return(0) }}
    {% endif %}
{% endmacro %}

{% macro validate_string_column_anomalies() %}
    {%- set max_bucket_end = elementary.quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
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
    {%- set max_bucket_end = elementary.quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
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
    {%- set max_bucket_end = elementary.quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}
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
    {%- set max_bucket_end = elementary.quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}

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
    {%- set max_bucket_end = elementary.quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    {% set alerts_relation = ref('alerts_dbt_tests') %}

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

{% macro validate_error_model() %}
    {%- set max_bucket_end = elementary.quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    {% set alerts_relation = ref('alerts_dbt_models') %}

    {% set error_model_validation_query %}
        select distinct status
        from {{ alerts_relation }}
        where status = 'error' and materialization != 'snapshot'
        and detected_at >= {{ max_bucket_end }}
    {% endset %}
    {% set results = elementary.result_column_to_list(error_model_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['error']) }}
{% endmacro %}

{% macro validate_error_snapshot() %}
    {%- set max_bucket_end = elementary.quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    {% set alerts_relation = ref('alerts_dbt_models') %}

    {% set error_snapshot_validation_query %}
        select distinct status
        from {{ alerts_relation }}
        where status = 'error' and materialization = 'snapshot'
        and detected_at >= {{ max_bucket_end }}
    {% endset %}
    {% set results = elementary.result_column_to_list(error_snapshot_validation_query) %}
    {{ assert_lists_contain_same_items(results, ['error']) }}
{% endmacro %}

{% macro validate_schema_changes() %}
    {% set expected_changes = {('schema_changes', 'red_cards'): 'column_added',
                               ('schema_changes', 'group_a'):   'column_removed',
                               ('schema_changes', 'goals'):   'type_changed',
                               ('schema_changes', 'key_crosses'): 'column_added',
                               ('schema_changes', 'offsides'): 'column_removed',
                               ('schema_changes_from_baseline', 'group_b'): 'type_changed',
                               ('schema_changes_from_baseline', 'group_d'): 'column_added',
                               ('schema_changes_from_baseline', 'goals'): 'type_changed',
                               ('schema_changes_from_baseline', 'coffee_cups_consumed'): 'column_removed'
                               } %}
    {%- set max_bucket_end = elementary.quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    {% set alerts_relation = ref('alerts_schema_changes') %}
    {% set schema_changes_alerts %}
    select test_short_name, column_name, sub_type
    from {{ alerts_relation }}
        where detected_at >= {{ max_bucket_end }} and column_name is not NULL
    group by 1,2,3
    {% endset %}
    {% set alert_rows = run_query(schema_changes_alerts) %}
    {% set found_schema_changes = {} %}
    {% for row in alert_rows %}
        {% set test_short_name = row[0] | lower %}
        {% set column_name = row[1] | lower %}
        {% set alert = row[2] | lower %}
        {% if (test_short_name, column_name) not in expected_changes %}
            {% do elementary.edr_log("FAILED: " ~ test_short_name ~ " - could not find expected alert for " ~ column_name ~ ", " ~ alert) %}
        {% endif %}
        {% if expected_changes[(test_short_name, column_name)] != alert %}
            {% do elementary.edr_log("FAILED: " ~ test_short_name ~ " - for column " ~ column_name ~ " expected alert type " ~ expected_changes[(test_short_name, column_name)] ~ " but got " ~ alert) %}
            {{ return(1) }}
        {% endif %}
        {% do found_schema_changes.update({(test_short_name, column_name): alert}) %}

    {% endfor %}
    {% if found_schema_changes %}
        {%- set missing_changes = [] %}
        {%- for expected_change in expected_changes %}
            {%- if expected_change not in found_schema_changes %}
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
    {%- set max_bucket_end = elementary.quote(elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")) %}
    {% set alerts_relation = ref('alerts_dbt_tests') %}
    {% set dbt_test_alerts %}
        select table_name, column_name, test_name
        from {{ alerts_relation }}
            where detected_at >= {{ max_bucket_end }}
        group by 1, 2, 3
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
    {{ assert_list1_in_list2(['error_model', 'string_column_anomalies', 'numeric_column_anomalies', 'any_type_column_anomalies', 'any_type_column_anomalies_validation', 'numeric_column_anomalies_training'], found_tables) }}
    {{ assert_list1_in_list2(['missing_column', 'min_length', 'null_count_int'], found_columns) }}
    {{ assert_list1_in_list2(['uniques', 'relationships', 'singular_test_with_no_ref', 'singular_test_with_one_ref', 'singular_test_with_two_refs', 'singular_test_with_source_ref', 'generic_test_on_model', 'generic_test_on_column'], found_tests) }}

{% endmacro %}

{% macro validate_dbt_artifacts() %}
    {% set dbt_models_relation = ref('dbt_models') %}
    {% set dbt_models_query %}
        select distinct name from {{ dbt_models_relation }}
    {% endset %}
    {% set models = elementary.result_column_to_list(dbt_models_query) %}
    {{ assert_value_in_list('any_type_column_anomalies', models, context='dbt_models') }}
    {{ assert_value_in_list('numeric_column_anomalies', models, context='dbt_models') }}
    {{ assert_value_in_list('string_column_anomalies', models, context='dbt_models') }}

    {% set dbt_sources_relation = ref('dbt_sources') %}
    {% set dbt_sources_query %}
        select distinct name from {{ dbt_sources_relation }}
    {% endset %}
    {% set sources = elementary.result_column_to_list(dbt_sources_query) %}
    {{ assert_value_in_list('any_type_column_anomalies_training', sources, context='dbt_sources') }}
    {{ assert_value_in_list('string_column_anomalies_training', sources, context='dbt_sources') }}
    {{ assert_value_in_list('any_type_column_anomalies_validation', sources, context='dbt_sources') }}

    {% set dbt_tests_relation = ref('dbt_tests') %}
    {% set dbt_tests_query %}
        select distinct name from {{ dbt_tests_relation }}
    {% endset %}
    {% set tests = elementary.result_column_to_list(dbt_tests_query) %}

    {% set dbt_run_results = ref('dbt_run_results') %}
    {% set dbt_run_results_query %}
        select distinct name from {{ dbt_run_results }} where resource_type in ('model', 'test')
    {% endset %}
    {% set run_results = elementary.result_column_to_list(dbt_run_results_query) %}
    {% set all_executable_nodes = [] %}
    {% do all_executable_nodes.extend(models) %}
    {% do all_executable_nodes.extend(tests) %}
    {{ assert_list1_in_list2(run_results, all_executable_nodes, context='dbt_run_results') }}
{% endmacro %}

{% macro validate_source_freshness() %}
    {% set query %}
      select status from {{ ref('dbt_source_freshness_results') }}
    {% endset %}
    {% set results = elementary.result_column_to_list(query) %}
    {{ assert_lists_contain_same_items(results, ['warn', 'error', 'runtime error']) }}
{% endmacro %}
