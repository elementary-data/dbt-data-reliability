{% macro first_value(column) %}
    {{ return(adapter.dispatch("first_value", "elementary")(column)) }}
{% endmacro %}

{% macro default__first_value(column) %} first_value({{ column }}) {% endmacro %}

{#
  ClickHouse's plain first_value ignores the window frame, so the frame-aware
  variant is required to read the earliest value within the frame.
  Mirrors the lagInFrame handling in lag.sql.
#}
{% macro clickhouse__first_value(column) %}
    first_valueinframe({{ column }})
{% endmacro %}
