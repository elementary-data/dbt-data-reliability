{% macro query_table_metrics() %}
  {% set query %}
    select
      {{ modules.datetime.datetime.utcnow().timestamp() }} as build_timestamp,
      count(*) as row_count
    from {{ this }}
  {% endset %}

  {% set metrics = [] %}
  {% for metric_column in elementary.run_query(query).columns %}
    {% set metric_name = metric_column.name %}
    {% set metric_value = metric_column[0] %}
    {% do metrics.append({
      "id": "{}.{}".format(invocation_id, this),
      "full_table_name": elementary.relation_to_full_name(this),
      "column_name": none,
      "metric_name": metric_name,
      "metric_value": metric_value,
      "updated_at": elementary.datetime_now_utc_as_string()
    }) %}
  {% endfor %}
  {% do return(metrics) %}
{% endmacro %}

{% macro bigquery__query_table_metrics() %}
  {% set query %}
    select
      {{ modules.datetime.datetime.utcnow().timestamp() }} as build_timestamp,
      total_rows AS row_count
    from {{ this.database }}.{{ this.schema }}.INFORMATION_SCHEMA.TABLE_STORAGE
    where table_id = '{{ this.name }}'
  {% endset %}

  {% set metrics = [] %}
  {% for metric_column in elementary.run_query(query).columns %}
    {% set metric_name = metric_column.name %}
    {% if metric_column | length == 0 %}
      {% set metric_value = none %} {# this shouldn't happen, but mainly to avoid edge cases / never fail here #}
    {% else %}
      {% set metric_value = metric_column[0] %}
    {% endif %}
    {% do metrics.append({
      "id": "{}.{}".format(invocation_id, this),
      "full_table_name": elementary.relation_to_full_name(this),
      "column_name": none,
      "metric_name": metric_name,
      "metric_value": metric_value,
      "updated_at": elementary.datetime_now_utc_as_string()
    }) %}
  {% endfor %}
  {% do return(metrics) %}
{% endmacro %}
  
{% macro can_query_metrics() %}
  {% if not elementary.get_config_var('collect_metrics') %}
    {% do return(false) %}
  {% endif %}

  {% if model.config.require_partition_filter %}
    {% do return(false) %}
  {% endif %}

  {% do return(true) %}
{% endmacro %}

{% macro query_metrics() %}
  {% if not elementary.can_query_metrics() %}
    {% do return([]) %}
  {% endif %}

  {% do return(elementary.query_table_metrics()) %}
{% endmacro %}

{% macro cache_metrics(metrics) %}
  {% do elementary.get_cache("tables").get("metrics").get("rows").extend(metrics) %}
{% endmacro %}
