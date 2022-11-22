{# The macro only works if using `package` in `packages.yml`, rather than `git`, `local`, etc. #}
{% macro get_elementary_package_version() %}
  {% set conf = elementary.get_runtime_config() %}
  {% set packages = conf.packages and conf.packages.packages %}
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
