{% macro get_row_count_metric() %}
  {% set query %}
    select count(*) as row_count
    from {{ this }}
  {% endset %}
  {% set value = elementary.result_value(query) %}
  {% do return({
    "id": "{}.{}".format(invocation_id, this),
    "full_table_name": this | string,
    "column_name": none,
    "metric_name": "row_count",
    "metric_value": value
  }) %}
{% endmacro %}

{% macro query_metrics() %}
  {% do return([
    elementary.get_row_count_metric()
  ]) %}
{% endmacro %}

{% macro cache_metrics(metrics) %}
  {% do elementary.get_cache("tables").get("metrics").extend(metrics) %}
{% endmacro %}
