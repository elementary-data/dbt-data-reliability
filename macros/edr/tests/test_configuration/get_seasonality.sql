{% macro get_seasonality(seasonality, model_graph_node, time_bucket, timestamp_column) %}
    {%- set seasonality = elementary.get_test_argument('seasonality', seasonality, model_graph_node) %}
    {%- do elementary.validate_seasonality(seasonality, time_bucket, timestamp_column) -%}
    {{ return(seasonality) }}
{% endmacro %}

{% macro validate_seasonality(seasonality, time_bucket, timestamp_column) %}
    {% if seasonality %}
      {% if not timestamp_column %}
        {% do exceptions.raise_compiler_error('Test with seasonality must have a timestamp_column, but none was provided') %}
      {% endif %}
      {% set supported_seasonality_values = ['day_of_week'] %}
      {% if seasonality not in supported_seasonality_values %}
        {% do exceptions.raise_compiler_error('Seasonality value should be one of' ~ supported_seasonality_values ~ ', got ' ~ seasonality ~ ' instead') %}
      {% endif %}
      {% if (time_bucket.count != 1) or (time_bucket.period != 'day') %}
        {% do exceptions.raise_compiler_error('Seasonality is supported only with time_bucket 1 day, got period: ' ~ time_bucket.period ~ ' and count: ' ~ time_bucket.count ~ ' instead') %}
      {% endif %}
    {% endif %}
{% endmacro %}