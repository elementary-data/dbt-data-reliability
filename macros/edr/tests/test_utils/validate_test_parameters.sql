{% macro validate_seasonality_parameter(seasonality, time_bucket, timestamp_column) %}
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

{% macro validate_directional_parameter(anomaly_direction) %}
    {% if anomaly_direction %}
      {% set direction_case_insensitive = anomaly_direction | lower %}
      {% if direction_case_insensitive not in ['drop','spike','both'] %}
        {% do exceptions.raise_compiler_error('Supported anomaly directions are: both, drop, spike. received anomaly_direction: {}'.format(anomaly_direction)) %}
      {% endif %}
    {% else %}
      {% do exceptions.raise_compiler_error('anomaly_direction can\'t be empty. Supported anomaly directions are: both, drop, spike') %}
    {% endif %}
{% endmacro %}