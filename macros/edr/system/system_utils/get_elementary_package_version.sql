{# Only works if package is pulled from the registry. #}

{% macro get_elementary_package_version() %}
  {% for pkg in ref.config.packages.packages %}
    {% if pkg.package == 'elementary-data/elementary' %}
      {{ return(pkg.version) }}
    {% endif %}
  {% endfor %}
  {{ return(none) }}
{% endmacro %}
