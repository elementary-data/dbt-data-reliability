{% macro get_result_node(identifier, package_name='elementary') %}
  {% for result in results %}
    {% set node = elementary.get_node(result.unique_id) %}
    {% if node.identifier == identifier %}
      {% if package_name %}
        {% if node.package_name == package_name %}
          {{ return(node) }}
        {% endif %}
      {% else %}
        {{ return(node) }}
      {% endif %}
    {% endif %}
  {% endfor %}
  {{ return(none) }}
{% endmacro %}
