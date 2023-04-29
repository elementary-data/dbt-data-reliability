{% macro validate_time_bucket(time_bucket) %}
    {% if time_bucket %}
      {% if time_bucket.count and time_bucket.count is not integer %}
        {% do exceptions.raise_compiler_error("time_bucket.count expectes valid integer, got: {} (If it's an integer, try to remove quotes)".format(time_bucket.count)) %}
      {% endif %}
      {% set supported_periods = ['hour','day','week','month'] %}
      {% if time_bucket.period and time_bucket.period not in supported_periods %}
        {% do exceptions.raise_compiler_error("time_bucket.period value should be one of {0}, got: {1}".format(supported_periods, time_bucket.period)) %}
      {% endif %}
    {% endif %}
{% endmacro %}