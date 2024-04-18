{% macro get_column_group_by(group_by) %}
  {% if group_by %}
    {{ return(group_by) }}
  {% endif %}

  {{ return([]) }}
{% endmacro %}