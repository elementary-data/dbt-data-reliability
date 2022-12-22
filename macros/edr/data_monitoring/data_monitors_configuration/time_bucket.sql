{% macro get_default_time_bucket() %}
  {% do return({"period": "day", "count": 1}) %}
{% endmacro %}

{% macro validate_time_bucket(time_bucket) %}
  {% if timebucket.period is not string %}
    {% do exceptions.raise_compiler_error("time_bucket.period must be a string (hour, day, week)") %}
  {% elif time_bucket.count is not number %}
    {% do exceptions.raise_compiler_error("time_bucket.count must be a number") %}
  {% endif %}
{% endmacro %}
