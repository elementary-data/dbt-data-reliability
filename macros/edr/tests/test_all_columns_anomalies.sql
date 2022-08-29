{% test all_columns_anomalies(model, column_anomalies = none, exclude_prefix = none, exclude_regexp = none, timestamp_column = none, sensitivity = none, backfill_days=none) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_anomaly_detection') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {%- set test_name_in_graph = elementary.get_test_name_in_graph() %}
        {{- elementary.debug_log('collecting metrics for test: ' ~ test_name_in_graph) }}
        {#- creates temp relation for test metrics -#}
        {%- set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set schema_name = schema_name ~ elementary.get_config_var('tests_schema_name') %}
        {%- set temp_metrics_table_name = elementary.table_name_with_suffix(test_name_in_graph, '__metrics') %}
        {{- elementary.debug_log('metrics table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_metrics_table_name) }}
        {%- set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_metrics_table_name,
                                                                                   type='table') -%}

        {#- get all columns configuration -#}
        {%- set full_table_name = elementary.relation_to_full_name(model) %}
        {%- set model_relation = dbt.load_relation(model) %}
        {%- if not model_relation %}
            {{- elementary.test_log('monitored_table_not_found', full_table_name) }}
            {{- return(elementary.no_results_query()) }}
        {%- endif %}

        {% set model_graph_node = elementary.get_model_graph_node(model_relation) %}
        {% set timestamp_column = elementary.get_timestamp_column(timestamp_column, model_graph_node) %}

        {%- set timestamp_column_data_type = elementary.find_normalized_data_type_for_column(model, timestamp_column) %}
        {{ elementary.debug_log('timestamp_column - ' ~ timestamp_column) }}
        {{ elementary.debug_log('timestamp_column_data_type - ' ~ timestamp_column_data_type) }}
        {%- set is_timestamp = elementary.get_is_column_timestamp(model_relation, timestamp_column, timestamp_column_data_type) %}
        {{- elementary.debug_log('is_timestamp - ' ~ is_timestamp) }}

        {%- set column_objs_and_monitors = elementary.get_all_column_obj_and_monitors(model_relation, column_anomalies) -%}
        {% set backfill_days = elementary.get_test_argument(argument_name='backfill_days', value=backfill_days) %}
        {#- execute table monitors and write to temp test table -#}
        {%- set monitors = [] %}
        {%- if column_objs_and_monitors | length > 0 %}
            {{- elementary.test_log('start', full_table_name, 'all columns') }}
            {%- set empty_table_query = elementary.empty_data_monitoring_metrics() %}
            {%- do elementary.create_or_replace(False, temp_table_relation, empty_table_query) %}
            {%- for column_obj_and_monitors in column_objs_and_monitors %}
                {%- set column_obj = column_obj_and_monitors['column'] %}
                {%- set column_monitors = column_obj_and_monitors['monitors'] %}
                {%- set column_name = column_obj.name -%}
                {%- set ignore_column = elementary.should_ignore_column(column_name, exclude_regexp, exclude_prefix) -%}
                {%- if not ignore_column -%}
                    {%- do monitors.extend(column_monitors) -%}
                    {%- set min_bucket_start = "'" ~ elementary.get_min_bucket_start(full_table_name, backfill_days, column_monitors, column_name) ~ "'" %}
                    {{ elementary.debug_log('min_bucket_start - ' ~ min_bucket_start) }}
                    {{ elementary.test_log('start', full_table_name, column_name) }}
                    {%- set column_monitoring_query = elementary.column_monitoring_query(model_relation, timestamp_column, is_timestamp, min_bucket_start, column_obj, column_monitors) %}
                    {%- do run_query(elementary.insert_as_select(temp_table_relation, column_monitoring_query)) -%}
                {%- else -%}
                    {{ elementary.debug_log('column ' ~ column_name ~ ' is excluded') }}
                {%- endif -%}
            {%- endfor %}
        {%- endif %}
        {%- set all_columns_monitors = monitors | unique | list %}
        {#- query if there is an anomaly in recent metrics -#}
        {%- set sensitivity = elementary.get_test_argument(argument_name='anomaly_sensitivity', value=sensitivity) %}
        {%- set anomaly_scores_query = elementary.get_anomaly_scores_query(temp_table_relation, full_table_name, sensitivity, backfill_days, all_columns_monitors, columns_only=true) %}
        {%- set anomaly_scores_test_table_name = elementary.table_name_with_suffix(test_name_in_graph, '__anomaly_scores') %}
        {{- elementary.debug_log('anomalies table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ anomaly_scores_test_table_name) }}
        {%- set anomaly_scores_test_table_exists, anomaly_scores_test_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=anomaly_scores_test_table_name,
                                                                                   type='table') -%}
        {%- do elementary.create_or_replace(False, anomaly_scores_test_table_relation, anomaly_scores_query) %}

        {{- elementary.test_log('end', full_table_name, 'all columns') }}

        {# return anomalies query as standard test query #}
        {{ elementary.get_anomaly_query(anomaly_scores_test_table_relation, sensitivity, backfill_days) }}

    {%- else %}

        {#- test must run an sql query -#}
        {{- elementary.no_results_query() }}

    {%- endif %}
{% endtest %}

{%- macro should_ignore_column(column_name, exclude_regexp, exclude_prefix) -%}
    {%- set regex_module = modules.re -%}
    {%- if exclude_regexp -%}
        {%- set is_match = regex_module.match(exclude_regexp, column_name, regex_module.IGNORECASE) %}
        {%- if is_match -%}
            {{ return(True) }}
        {%- endif -%}
    {%- endif -%}
    {% if exclude_prefix %}
        {%- set exclude_regexp = '^' ~ exclude_prefix ~ '.*' %}
        {%- set is_match = regex_module.match(exclude_regexp, column_name, regex_module.IGNORECASE) %}
        {%- if is_match -%}
            {{ return(True) }}
        {%- endif -%}
    {%- endif -%}
    {{ return(False) }}
{%- endmacro -%}

