-- TODO: Add validation for backfill days, sensitivity and min_training_size?
-- TODO: Add min_training_set_size to tests and use it
-- TODO: Add monitors to this macro
-- TODO: Add min and max time buckets to be in the config as well
-- TODO: Add tests specific config to be part of the config as well
-- TODO: Add validation that mandatory config to run the test was passed

{% macro get_anomalies_test_configuration(model_relation,
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
                                          dimensions) %}

    {%- set model_graph_node = elementary.get_model_graph_node(model_relation) %}

    {# All anomaly detection tests #}
    {%- set timestamp_column = elementary.get_timestamp_column(timestamp_column, model_graph_node, model_relation) %}
    {%- set where_expression = elementary.get_test_argument('where_expression', where_expression, model_graph_node) %}
    {%- set anomaly_sensitivity = elementary.get_test_argument('anomaly_sensitivity', anomaly_sensitivity, model_graph_node) %}
    {%- set anomaly_direction = elementary.get_anomaly_direction(anomaly_direction, model_graph_node) | lower %}
    {%- set min_training_set_size = elementary.get_test_argument('min_training_set_size', min_training_set_size, model_graph_node) %}

    {# timestamp_column anomaly detection tests #}
    {%- set time_bucket = elementary.get_time_bucket(time_bucket, model_graph_node) %}
    {%- set days_back = elementary.get_days_back(days_back, model_graph_node, seasonality) %}
    {%- set backfill_days = elementary.get_test_argument('backfill_days', backfill_days, model_graph_node) %}
    {%- set seasonality = elementary.get_seasonality(seasonality, model_graph_node, time_bucket, timestamp_column) | lower %}

    {% set anomalies_test_configuration_dict =
      {'timestamp_column': (timestamp_column if timestamp_column else None),
       'where_expression': (where_expression if where_expression else None),
       'anomaly_sensitivity': (anomaly_sensitivity if anomaly_sensitivity else None),
       'anomaly_direction': (anomaly_direction if anomaly_direction else None),
       'min_training_set_size': (min_training_set_size if min_training_set_size else None),
       'time_bucket': (time_bucket if time_bucket else None) ,
       'days_back': (days_back if days_back else None) ,
       'backfill_days':(backfill_days if backfill_days else None),
       'seasonality':(seasonality if seasonality else None)
        } %}

  {# Changes in these configs impact the metric id of the test. #}
  {# If these configs change, we ignore the old metrics and recalculate. #}
    {% set metric_properties_dict =
      {'timestamp_column': (timestamp_column if timestamp_column else None) ,
       'where_expression': (where_expression if where_expression else None) ,
       'time_bucket': (time_bucket if time_bucket else None),
       'freshness_column': (freshness_column if freshness_column else None) ,
       'event_timestamp_column':(event_timestamp_column if event_timestamp_column else None),
       'dimensions':(dimensions if dimensions else None)
        } %}

   {# Adding to cache so test configuration will be available outside the test context #}
    {%- set test_unique_id = elementary.get_test_unique_id() %}
    {%- do elementary.set_cache(test_unique_id, anomalies_test_configuration_dict) -%}

    {{ return([anomalies_test_configuration_dict, metric_properties_dict]) }}
{% endmacro %}
