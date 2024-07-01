-- TODO: Add validation for backfill days, sensitivity and min_training_size?
-- TODO: Add monitors as well?
-- TODO: Add min and max time buckets to be in the config as well?

{% macro get_anomalies_test_configuration(model_relation,
                                          mandatory_params,
                                          timestamp_column,
                                          where_expression,
                                          anomaly_sensitivity,
                                          anomaly_direction,
                                          min_training_set_size,
                                          time_bucket,
                                          days_back,
                                          backfill_days,
                                          seasonality,
                                          freshness_column,
                                          event_timestamp_column,
                                          dimensions,
                                          sensitivity,
                                          ignore_small_changes,
                                          fail_on_zero,
                                          detection_delay,
                                          anomaly_exclude_metrics,
                                          detection_period,
                                          training_period,
                                          exclude_final_results) %}

    {%- set model_graph_node = elementary.get_model_graph_node(model_relation) %}
    {# Changes in these configs impact the metric id of the test. #}
    {# If these configs change, we ignore the old metrics and recalculate. #}
    {% set metric_props = elementary.get_metric_properties(model_graph_node, timestamp_column, where_expression, time_bucket, dimensions, freshness_column, event_timestamp_column) %}
    {% set metric_props = elementary.undefined_dict_keys_to_none(metric_props) %}


    {# All anomaly detection tests #}
    {# We had a names mix in sensitivity/anomaly_sensitivity, this keeps backwards competability #}
    {%- set anomaly_sensitivity = sensitivity if sensitivity else elementary.get_test_argument('anomaly_sensitivity', anomaly_sensitivity, model_graph_node) %}
    {%- set anomaly_direction = elementary.get_anomaly_direction(anomaly_direction, model_graph_node) %}
    {%- set detection_period = elementary.get_test_argument('detection_period', detection_period, model_graph_node) -%}
    {%- set backfill_days = elementary.detection_period_to_backfill_days(detection_period, backfill_days, model_graph_node) -%}
    {%- set fail_on_zero = elementary.get_test_argument('fail_on_zero', fail_on_zero, model_graph_node) %}
    

    {# timestamp_column anomaly detection tests #}
    {%- set seasonality = elementary.get_seasonality(seasonality, model_graph_node, metric_props.time_bucket, metric_props.timestamp_column) %}
    {%- set training_period = elementary.get_test_argument('training_period', training_period, model_graph_node) -%}
    {%- set days_back = elementary.training_period_to_days_back(training_period, days_back, model_graph_node) -%}
    {%- set days_back = elementary.get_days_back(days_back, model_graph_node, seasonality) %}
    {%- set detection_delay = elementary.get_detection_delay(detection_delay, model_graph_node) %}

    {%- set ignore_small_changes = elementary.get_test_argument('ignore_small_changes', ignore_small_changes, model_graph_node) %}
    {# Validate ignore_small_changes #}

    {% set anomaly_exclude_metrics = elementary.get_test_argument('anomaly_exclude_metrics', anomaly_exclude_metrics, model_graph_node) %}
    {% set exclude_final_results = elementary.get_exclude_final_results(exclude_final_results) %}

    {% set test_configuration =
      {'timestamp_column': metric_props.timestamp_column,
       'where_expression': metric_props.where_expression,
       'time_bucket': metric_props.time_bucket,
       'freshness_column': metric_props.freshness_column,
       'event_timestamp_column': metric_props.event_timestamp_column,
       'dimensions': metric_props.dimensions,

       'days_back': days_back,
       'backfill_days': backfill_days,
       'anomaly_sensitivity': anomaly_sensitivity,
       'anomaly_direction': anomaly_direction,
       'seasonality': seasonality,
       'ignore_small_changes': ignore_small_changes,
       'fail_on_zero': fail_on_zero,
       'detection_delay': detection_delay,
       'anomaly_exclude_metrics': anomaly_exclude_metrics,
       'exclude_final_results': exclude_final_results
        } %}
    {%- set test_configuration = elementary.undefined_dict_keys_to_none(test_configuration) -%}
    {%- do elementary.validate_mandatory_configuration(test_configuration, mandatory_params) -%}
    {%- do elementary.validate_ignore_small_changes(test_configuration) -%}

  {# Adding to cache so test configuration will be available outside the test context #}
    {%- set test_unique_id = elementary.get_test_unique_id() %}
    {%- do elementary.set_cache(test_unique_id, test_configuration) -%}
    {{ return([test_configuration, metric_props]) }}
{% endmacro %}

{% macro validate_mandatory_configuration(test_configuration, mandatory_params) %}
    {%- set mandatory_configuration = ['anomaly_sensitivity', 'anomaly_direction', 'backfill_days'] %}
    {%- set with_timestamp_mandatory_configuration = ['time_bucket', 'days_back'] %}
    {%- set missing_mandatory_params = [] %}

    {%- if mandatory_params and mandatory_params is iterable %}
        {%- set mandatory_configuration = elementary.union_lists(mandatory_configuration, mandatory_params) %}
    {%- endif %}
    {%- if test_configuration.timestamp_column %}
        {%- set mandatory_configuration = elementary.union_lists(mandatory_configuration, with_timestamp_mandatory_configuration) %}
    {%- endif %}

    {%- for mandatory_param in mandatory_configuration %}
        {%- if not test_configuration.get(mandatory_param) %}
            {%- do missing_mandatory_params.append(mandatory_param) -%}
        {%- endif %}
    {%- endfor %}
    {%- if missing_mandatory_params | length > 0 %}
        {% do exceptions.raise_compiler_error('Missing mandatory configuration: {}'.format(missing_mandatory_params)) %}
    {%- endif %}
{% endmacro %}

{% macro validate_ignore_small_changes(test_configuration) %}
    {% for key, value in test_configuration.ignore_small_changes.items() %}
        {% if key not in ['spike_failure_percent_threshold', 'drop_failure_percent_threshold'] %}
          {% do exceptions.raise_compiler_error('Illegal configuration key: {}'.format(key)) %}
        {% endif %}

        {% if (value is not none) and (value | int) and (value >= 0) %}
          {% if (value | int) == 0 or value < 0 %}
            {% do exceptions.raise_compiler_error('Illegal value for ignore_small_changes config: {}'.format(value)) %}
          {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
