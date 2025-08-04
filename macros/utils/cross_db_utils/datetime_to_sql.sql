{% macro edr_datetime_to_sql(dt) %}
  {% do return(adapter.dispatch("edr_datetime_to_sql", "elementary")(dt)) %}
{% endmacro %}

{% macro default__edr_datetime_to_sql(dt) %}
  {% do return(elementary.edr_quote(dt)) %}
{% endmacro %}

{% macro dremio__edr_datetime_to_sql(dt) %}
  {% if dt is string %}
    {% set dt = modules.datetime.datetime.fromisoformat(dt) %}
  {% endif %}
  {% do return(elementary.edr_quote(dt.strftime(elementary.get_time_format()))) %}
{% endmacro %}
