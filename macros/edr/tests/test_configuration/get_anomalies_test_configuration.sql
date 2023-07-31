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
                                          sensitivity) %}

    {%- set model_graph_node = elementary.get_model_graph_node(model_relation) %}

    {# All anomaly detection tests #}
    {%- set timestamp_column = elementary.get_test_argument('timestamp_column', timestamp_column, model_graph_node) %}
    {%- set where_expression = elementary.get_test_argument('where_expression', where_expression, model_graph_node) %}
    {# We had a names mix in sensitivity/anomaly_sensitivity, this keeps backwards competability #}
    {%- set anomaly_sensitivity = sensitivity if sensitivity else elementary.get_test_argument('anomaly_sensitivity', anomaly_sensitivity, model_graph_node) %}
    {%- set anomaly_direction = elementary.get_anomaly_direction(anomaly_direction, model_graph_node) %}
    {%- set min_training_set_size = elementary.get_test_argument('min_training_set_size', min_training_set_size, model_graph_node) %}
    {%- set backfill_days = elementary.get_test_argument('backfill_days', backfill_days, model_graph_node) %}

    {# timestamp_column anomaly detection tests #}
    {%- set time_bucket = elementary.get_time_bucket(time_bucket, model_graph_node) %}
    {%- set days_back = elementary.get_days_back(days_back, model_graph_node, seasonality) %}
    {%- set seasonality = elementary.get_seasonality(seasonality, model_graph_node, time_bucket, timestamp_column) %}

    {% set test_configuration =
      {'timestamp_column': timestamp_column,
       'where_expression': where_expression,
       'anomaly_sensitivity': anomaly_sensitivity,
       'anomaly_direction': anomaly_direction,
       'min_training_set_size': min_training_set_size,
       'time_bucket': time_bucket,
       'days_back': days_back,
       'backfill_days': backfill_days,
       'seasonality': seasonality,
       'freshness_column': freshness_column,
       'event_timestamp_column': event_timestamp_column,
       'dimensions': dimensions
        } %}
    {%- set test_configuration = elementary.empty_dict_keys_to_none(test_configuration) -%}
    {%- do elementary.validate_mandatory_configuration(test_configuration, mandatory_params) -%}

  {# Changes in these configs impact the metric id of the test. #}
  {# If these configs change, we ignore the old metrics and recalculate. #}
    {% set metric_properties =
      {'timestamp_column': timestamp_column,
       'where_expression': where_expression,
       'time_bucket': time_bucket,
       'freshness_column': freshness_column,
       'event_timestamp_column': event_timestamp_column,
       'dimensions': dimensions
        } %}
    {%- set metric_properties = elementary.empty_dict_keys_to_none(metric_properties) -%}

  {# Adding to cache so test configuration will be available outside the test context #}
    {%- set test_unique_id = elementary.get_test_unique_id() %}
    {%- do elementary.set_cache(test_unique_id, test_configuration) -%}

    {{ return([test_configuration, metric_properties]) }}
{% endmacro %}


{% macro validate_mandatory_configuration(test_configuration, mandatory_params) %}
    {%- set mandatory_configuration = ['anomaly_sensitivity', 'anomaly_direction', 'min_training_set_size', 'backfill_days'] %}
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