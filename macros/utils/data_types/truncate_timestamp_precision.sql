{#
  Truncates sub-microsecond fractional digits from a timestamp string, e.g.
  '2026-04-03T10:50:50.961498756Z' -> '2026-04-03T10:50:50.961498Z'.
  Some runtimes (e.g. dbt-fusion, dbt-core 1.11) produce nanosecond-precision timing values,
  which BigQuery's microsecond TIMESTAMP cannot cast. Non-string values are returned unchanged.
  Used on upload so stored timing values are castable by any reader with a plain cast,
  including readers we don't control (users' own queries, the cloud sync).
  Rows written before this truncation existed are handled by the
  bigquery_truncate_nanosecond_timestamps var, or fixed in place by fix_nanosecond_timing_values.
#}
{% macro truncate_timestamp_precision(value) %}
    {% if value is not string %} {% do return(value) %} {% endif %}
    {% set match = modules.re.search("^(.*\.\d{6})\d+(.*)$", value) %}
    {% if match %} {% do return(match.group(1) ~ match.group(2)) %} {% endif %}
    {% do return(value) %}
{% endmacro %}
