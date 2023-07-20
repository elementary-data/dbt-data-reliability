{% macro query_row_count_metric() %}
  {% set query %}
    select count(*) as row_count
    from {{ this }}
  {% endset %}
  {% do return(elementary.result_value(query)) %}
{% endmacro %}

{% macro cache_row_count_metric(row_count) %}
  {% do elementary.get_cache("tables").get("metrics").append({
    "id": "{}.{}".format(invocation_id, this),
    "full_table_name": this | string,
    "column_name": none,
    "metric_name": "row_count",
    "metric_value": row_count
  }) %}
{% endmacro %}
