{% macro get_default_time_bucket() %}
  {% do return({"period": "day", "count": 1}) %}
{% endmacro %}
