{% macro detection_period_to_backfill_days(detection_period, backfill_days, model_graph_node) %}
  {% if detection_period %}
    {{ return(elementary.convert_period(detection_period, "day").count) }}
  {% endif %}

  {{ return(elementary.get_test_argument('backfill_days', backfill_days, model_graph_node)) }}
{% endmacro %}

{% macro training_period_to_days_back(training_period, days_back, model_graph_node) %}
  {% if training_period %}
    {{ return(elementary.convert_period(training_period, "day").count) }}
  {% endif %}

  {{ return(elementary.get_test_argument('days_back', days_back, model_graph_node)) }}
{% endmacro %}

{% macro get_period_default_var(unit, count) %}
  {{ return({'unit': unit, 'count': count}) }}
{% endmacro %}

{% macro get_unit_of_period(period) %}
  {{ return(period.unit) }}
{% endmacro %}

{% macro get_count_of_period(period) %}
  {{ return(period.count) }}
{% endmacro %}

{% macro convert_period(period, convert_to) %}
    {% set convert_from = elementary.get_unit_of_period(period) %}
  {% set period_count = elementary.get_count_of_period(period) %}
  
  {% if convert_from == 'week' %}
    {% if convert_to == 'day' %}
      {% set period_count = period_count * 7 %}
    {% elif convert_to == 'hour' %}
      {% set period_count = period_count * 7 * 24 %}
    {% elif convert_to == 'month' %}
      {% set period_count = (period_count * 7) / 30 %}
    {% endif %}
  
  {% elif convert_from == 'day' %}
    {% if convert_to == 'hour' %}
      {% set period_count = period_count * 24 %}
    {% elif convert_to == 'week '%}
      {% set period_count = period_count / 7 %}
    {% elif convert_to == 'month' %}
      {% set period_count = period_count / 30 %}
    {% endif %}

  {% elif convert_from == 'hour' %}
    {% if convert_to == 'day' %}
      {% set period_count = period_count / 24 %}
    {% elif convert_to == 'week' %}
      {% set period_count = period_count / (7 * 24) %}
    {% elif convert_to == 'month' %}
      {% set period_count = period_count / (24 * 30) %}
    {% endif %}

    {% elif convert_from == 'month' %}
    {% if convert_to == 'day' %}
      {% set period_count = period_count * 30 %}
    {% elif convert_to == 'week' %}
      {% set period_count = (period_count * 30) / 7  %}
    {% elif convert_to == 'hour' %}
      {% set period_count = period_count * 30 * 24 %}
    {% endif %}
  {% endif %}

  {% set period_count = period_count | int %}
  {{ return({'unit': convert_to, 'count': period_count}) }}
{% endmacro %}

{% macro months_to_days(count_of_months, end_datetime) %}
  {% set diff = value %}
  {{ return(end_datetime - modules.datetime.timedelta(months=count_of_months)) }}
{% endmacro %}

{% macro check() %}
  {{ log("### Start Check ###", true) }}
  {# {{ log(get_count_of_period({"count": 4}), true) }} #}
  {{ log(convert_period({"unit": "month", "count": 1}, "day"), true) }}
  {{ log("### End Check ###", true) }}
{% endmacro %}