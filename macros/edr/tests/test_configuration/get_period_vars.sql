{% macro detection_period_to_backfill_days(detection_period, backfill_days, model_graph_node) %}
  {% if detection_period %}
    {% if elementary.get_count_of_period(detection_period) != (elementary.get_count_of_period(detection_period) | int) %}
      {%- do elementary.edr_log_warning("Got a float number value in detection_period count. Rounding it down...") -%}
    {% endif %}

    {% if not detection_period.period %}
      {{ return(detection_period.count) }}
    {% endif %}

    {{ return(elementary.convert_period(detection_period, "day").count) }}
  {% endif %}

  {{ return(elementary.get_test_argument('backfill_days', backfill_days, model_graph_node)) }}
{% endmacro %}

{% macro training_period_to_days_back(training_period, days_back, model_graph_node) %}
  {% if training_period %}
    {% if elementary.get_count_of_period(training_period) != (elementary.get_count_of_period(training_period) | int) %}
      {%- do elementary.edr_log_warning("Got a float number value in training_period count. Rounding it down...") -%}
    {% endif %}

    {% if not training_period.period %}
      {{ return(training_period.count) }}
    {% endif %}

    {{ return(elementary.convert_period(training_period, "day").count) }}
  {% endif %}

  {{ return(elementary.get_test_argument('days_back', days_back, model_graph_node)) }}
{% endmacro %}

{% macro get_period_default_var(period, count) %}
  {{ return({'period': period, 'count': count}) }}
{% endmacro %}

{% macro get_unit_of_period(period_dict) %}
  {{ return(period_dict.period) }}
{% endmacro %}

{% macro get_count_of_period(period_dict) %}
  {{ return(period_dict.count) }}
{% endmacro %}

{% macro convert_period(period_dict, convert_to) %}
  {% set convert_from = elementary.get_unit_of_period(period_dict) %}
  {% set period_count = elementary.get_count_of_period(period_dict) %}
  
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
  {{ return({'period': convert_to, 'count': period_count}) }}
{% endmacro %}
