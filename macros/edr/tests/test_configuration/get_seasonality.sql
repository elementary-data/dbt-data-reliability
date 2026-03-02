{% macro get_seasonality(seasonality, model_graph_node, time_bucket, timestamp_column) %}
    {%- set seasonality = elementary.get_test_argument('seasonality', seasonality, model_graph_node) %}
    {%- do elementary.validate_seasonality(seasonality, time_bucket, timestamp_column) -%}
    {%- if seasonality %}
        {{ return(seasonality) }}
    {%- endif %}
    {{ return(none)}}
{% endmacro %}

{% macro validate_seasonality(seasonality, time_bucket, timestamp_column) %}
    {% if seasonality %}
      {% if not timestamp_column %}
        {% do exceptions.raise_compiler_error('Test with seasonality must have a timestamp_column, but none was provided') %}
      {% endif %}
      {% set supported_seasonality_values = ['day_of_week', 'hour_of_day', 'hour_of_week'] %}
      {%- set seasonality = seasonality | lower %}
      {% if seasonality not in supported_seasonality_values %}
        {% do exceptions.raise_compiler_error('Seasonality value should be one of' ~ supported_seasonality_values ~ ', got ' ~ seasonality ~ ' instead') %}
      {% endif %}
       {% if seasonality == 'day_of_week' and ((time_bucket.count != 1) or (time_bucket.period != 'day')) %}
        {% do exceptions.raise_compiler_error('Daily seasonality is supported only with time_bucket 1 day, got period: ' ~ time_bucket.period ~ ' and count: ' ~ time_bucket.count ~ ' instead') %}
      {% elif seasonality in ['hour_of_day', 'hour_of_week'] and ((time_bucket.count != 1) or (time_bucket.period != 'hour')) %}
        {% do exceptions.raise_compiler_error('Hourly seasonality is supported only with time_bucket 1 hour, got period: ' ~ time_bucket.period ~ ' and count: ' ~ time_bucket.count ~ ' instead') %}
      {% endif %}
    {% endif %}
{% endmacro %}
