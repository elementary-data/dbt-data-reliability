{% macro get_daily_time_bucket() %}
  {% do return({"period": "day", "count": 1}) %}
{% endmacro %}

{% macro get_default_time_bucket() %}
  {% do return(elementary.get_daily_time_bucket()) %}
{% endmacro %}

{% macro get_time_bucket(time_bucket, model_graph_node) %}
    {%- set configured_time_bucket = elementary.get_test_argument('time_bucket', time_bucket, model_graph_node) %}
    {%- do elementary.validate_time_bucket(configured_time_bucket) %}
    {%- set default_time_bucket = elementary.get_default_time_bucket() %}

    {%- if not configured_time_bucket %}
        {{ return(default_time_bucket) }}
    {%- elif configured_time_bucket and not configured_time_bucket.period %}
        {%- do configured_time_bucket.update({"period": default_time_bucket.period }) -%}
    {%- elif configured_time_bucket and not configured_time_bucket.count %}
        {%- do configured_time_bucket.update({"count": default_time_bucket.count }) %}
    {%- endif %}
    {{ return(configured_time_bucket) }}
{% endmacro %}

