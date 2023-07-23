{% macro query_table_metrics() %}
  {% set query %}
    select count(*) as row_count
    from {{ this }}
  {% endset %}

  {% set metrics = [] %}
  {% for metric_column in elementary.run_query(query).columns %}
    {% set metric_name = metric_column.name %}
    {% set metric_value = metric_column[0] %}
    {% do metrics.append({
      "id": "{}.{}".format(invocation_id, this),
      "full_table_name": this | string,
      "column_name": none,
      "metric_name": metric_name,
      "metric_value": metric_value 
    }) %}
  {% endfor %}
  {% do return(metrics) %}
{% endmacro %}

{% macro query_metrics() %}
  {% do return(elementary.query_table_metrics()) %}
{% endmacro %}

{% macro cache_metrics(metrics) %}
  {% do elementary.get_cache("tables").get("metrics").extend(metrics) %}
{% endmacro %}
