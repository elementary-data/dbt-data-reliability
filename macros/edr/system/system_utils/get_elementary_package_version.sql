{# The macro only works if using `package` in `packages.yml`, rather than `git`, `local`, etc. #}
{% macro get_elementary_package_version() %}
  {% set packages = ref.config and ref.config.packages and ref.config.packages.packages %}
  {% if not packages %}
    {{ return(none) }}
  {% endif %}
  {% for pkg in packages %}
    {% if pkg.package == 'elementary-data/elementary' %}
      {{ return(pkg.version) }}
    {% endif %}
  {% endfor %}
  {{ return(none) }}
{% endmacro %}
