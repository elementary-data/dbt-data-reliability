{% macro get_metrics_macro_map() %}
  {% do return(adapter.dispatch("get_metrics_macro_map", "elementary")()) %}
{% endmacro %}

{% macro default__get_metrics_macro_map() %}
    {% do return({
        "row_count": elementary.row_count_metric_query,
        "freshness": elementary.freshness_metric_query,
        "event_freshness": elementary.event_freshness_metric_query
    }) %}
{% endmacro %}
