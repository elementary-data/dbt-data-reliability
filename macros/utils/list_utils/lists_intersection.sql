{% macro lists_intersection(list_a, list_b) %}
  {% set new_list = [] %}
  {% for item in list_a %}
    {% if item in list_b %}
      {% do new_list.append(item) %}
    {% endif %}
  {% endfor %}
  {% do return(new_list) %}
{% endmacro %}
