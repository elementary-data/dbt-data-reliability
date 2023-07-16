{% macro generate_elementary_profile_parameters(method=none, overwrite_values=none) %}
  {#
    Returns a list of parameters for elementary profile.
    Each parameter consists of a list consisting of: 
      parameter name
      parameter value
      (optinal) parameter comment
  #}
  {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
  {% set parameters = return(adapter.dispatch('generate_elementary_profile_parameters', 'elementary')(method, elementary_database, elementary_schema)) %}
  {% if overwrite_values %}
    {% for parameter in parameters %}
      {% set parameter_name = parameter[0] %}
      {% if parameter_name in overwrite_values %}
        {% do parameter.insert(1, overwrite_values[parameter_name]) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro snowflake__generate_elementary_profile_parameters(method, elementary_database, elementary_schema) %}
  {% do return([
    ["type", target.type],
    ["account", target.account],
    ["user", target.user],
    ["password", "<PASSWORD>"],
    ["role", target.role],
    ["warehouse", target.warehouse],
    ["database", elementary_database],
    ["schema", elementary_schema],
    ["threads", target.threads],
  ]) %}
{% endmacro %}

{% macro bigquery__generate_elementary_profile_parameters(method, elementary_database, elementary_schema) %}
  {% set paremeters = [
    ["type", target.type],
    ["project", elementary_database],
    ["dataset", elementary_schema]
  ] %}
  {% if method == 'service-account' %}
    {% do paremeters.append(["method", "service-account"]) %}
    {% do paremeters.append(["keyfile", "<KEYFILE>"]) %}
  {% elif method == "github-actions" %}
    {% do paremeters.append(["method", "service-account"]) %}
    {% do paremeters.append(["keyfile", "/tmp/bigquery_keyfile.json", "Do not change this, supply `bigquery-keyfile` in `.github/workflows/elementary.yml`"]) %}
  {% else %}
    {% do paremeters.append(["method", "<AUTH_METHOD>", "Configure your auth method and add the required fields according to https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#authentication-methods"]) %}
  {% endif %}
  {% do paremeters.append(["threads", target.threads]) %}
  {% do return(paremeters) %}
{% endmacro %}

{% macro postgres__generate_elementary_profile_parameters(method, elementary_database, elementary_schema) %}
  {% do return([
    ["type", target.type],
    ["host", target.host],
    ["port", target.port],
    ["user", target.user],
    ["password", "<PASSWORD>"],
    ["dbname", elementary_database],
    ["schema", elementary_schema],
    ["threads", target.threads],
  ]) %}
{% endmacro %}

{% macro databricks__generate_elementary_profile_parameters(method, elementary_database, elementary_schema) %}
  {% set parameters = [
    ["type", target.type],
    ["host", target.host],
    ["http_path", target.http_path],
  ] %}
  {% if elementary_database %}
    {% do parameters.append(["catalog", elementary_database]) %}
  {% endif %}
  {% do parameters.extend([
    ["schema", elementary_schema],
    ["token", "<TOKEN>"],
    ["threads", target.threads],
  ]) %}
  {% do return(parameters) %}
{% endmacro %}

{% macro spark__generate_elementary_profile_parameters(method, elementary_database, elementary_schema) %}
  {% set parameters = [
    ["type", target.type],
    ["host", target.host],
    ["http_path", "<HTTP PATH>"]
  ] %}
  {% if elementary_database %}
    {% do parameters.append(["catalog", elementary_database]) %}
  {% endif %}
  {% do parameters.extend([
    ["schema", elementary_schema],
    ["token", "<TOKEN>"],
    ["threads", target.threads],
  ]) %}
  {% do return(parameters) %}
{% endmacro %}

{% macro default__generate_elementary_profile_parameters(method, elementary_database, elementary_schema) %}
Adapter "{{ target.type }}" is not supported on Elementary.
{% endmacro %}
