{% macro get_exclude_final_results(exclude_final_results_arg) %}
  {% if not exclude_final_results_arg %}
    {{ return("1 = 1") }}
  {% endif %}

  {{ return(exclude_final_results_arg) }}
{% endmacro %}