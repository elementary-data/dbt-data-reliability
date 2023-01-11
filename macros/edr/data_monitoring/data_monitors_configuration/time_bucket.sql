{% macro get_daily_time_bucket() %}
  {% do return({"period": "day", "count": 1}) %}
{% endmacro %}

{% macro get_default_time_bucket() %}
  {% do return(elementary.get_daily_time_bucket()) %}
{% endmacro %}
