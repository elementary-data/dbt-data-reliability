{% macro get_result_node(identifier, package_name='elementary') %}
  {% for result in results %}
    {% if result.node.identifier == identifier %}
      {% if package_name %}
        {% if result.node.package_name == package_name %}
          {{ return(result.node) }}
        {% endif %}
      {% else %}
        {{ return(result.node) }}
      {% endif %}
    {% endif %}
  {% endfor %}
  {{ return(none) }}
{% endmacro %}
