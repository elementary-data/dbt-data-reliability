{% macro generate_elementary_profile_args(method=none, overwrite_values=none) %}
  {#
    Returns a list of parameters for elementary profile.
    Each parameter consists of a dict consisting of:
      name
      value
      comment
  #}
  {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
  {% set parameters = adapter.dispatch('generate_elementary_profile_args', 'elementary')(method, elementary_database, elementary_schema) %}
  {% if overwrite_values %}
    {% for parameter in parameters %}
      {% if parameter["name"] in overwrite_values %}
        {% do parameter.update({"value": overwrite_values[parameter["name"]]}) %}
      {% endif %}
    {% endfor %}
  {% endif %}
  {% do return(parameters) %}
{% endmacro %}

{% macro _parameter(name, value, comment=none) %}
  {% do return({"name": name, "value": value, "comment": comment}) %}
{% endmacro %}

{% macro snowflake__generate_elementary_profile_args(method, elementary_database, elementary_schema) %}
  {% do return([
    _parameter("type", target.type),
    _parameter("account", target.account),
    _parameter("user", target.user),
    _parameter("private_key", "<contents of generated private key>"),
    _parameter("private_key_passphrase", "<passphrase for the private key, if key is encrypted>"),
    _parameter("role", target.role),
    _parameter("warehouse", target.warehouse),
    _parameter("database", elementary_database),
    _parameter("schema", elementary_schema),
    _parameter("threads", target.threads),
  ]) %}
{% endmacro %}

{% macro bigquery__generate_elementary_profile_args(method, elementary_database, elementary_schema) %}
  {% set parameters = [
    _parameter("type", target.type),
    _parameter("project", elementary_database),
    _parameter("dataset", elementary_schema)
  ] %}
  {% if method == 'service-account' %}
    {% do parameters.append(_parameter("method", "service-account")) %}
    {% do parameters.append(_parameter("keyfile", "<KEYFILE>")) %}
  {% elif method == "github-actions" %}
    {% do parameters.append(_parameter("method", "service-account")) %}
    {% do parameters.append(_parameter("keyfile", "/tmp/bigquery_keyfile.json", "Do not change this, supply `bigquery-keyfile` in `.github/workflows/elementary.yml`")) %}
  {% else %}
    {% do parameters.append(_parameter("method", "<AUTH_METHOD>", "Configure your auth method and add the required fields according to https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#authentication-methods")) %}
  {% endif %}
  {% do parameters.append(_parameter("threads", target.threads)) %}
  {% do return(parameters) %}
{% endmacro %}

{% macro postgres__generate_elementary_profile_args(method, elementary_database, elementary_schema) %}
  {% do return([
    _parameter("type", target.type),
    _parameter("host", target.host),
    _parameter("port", target.port),
    _parameter("user", target.user),
    _parameter("password", "<PASSWORD>"),
    _parameter("dbname", elementary_database),
    _parameter("schema", elementary_schema),
    _parameter("threads", target.threads),
  ]) %}
{% endmacro %}

{% macro clickhouse__generate_elementary_profile_args(method, elementary_database, elementary_schema) %}
  {% do return([
    _parameter("type", target.type),
    _parameter("host", target.host),
    _parameter("port", target.port),
    _parameter("user", target.user),
    _parameter("password", "<PASSWORD>"),
    _parameter("dbname", elementary_database),
    _parameter("schema", elementary_schema),
    _parameter("threads", target.threads),
  ]) %}
{% endmacro %}

{% macro databricks__generate_elementary_profile_args(method, elementary_database, elementary_schema) %}
  {% set parameters = [
    _parameter("type", target.type),
    _parameter("host", target.host),
    _parameter("http_path", target.http_path),
  ] %}
  {% if elementary_database %}
    {% do parameters.append(_parameter("catalog", elementary_database)) %}
  {% endif %}
  {% do parameters.extend([
    _parameter("schema", elementary_schema),
    _parameter("token", "<TOKEN>"),
    _parameter("threads", target.threads),
  ]) %}
  {% do return(parameters) %}
{% endmacro %}

{% macro spark__generate_elementary_profile_args(method, elementary_database, elementary_schema) %}
  {% set parameters = [
    _parameter("type", target.type),
    _parameter("host", target.host),
    _parameter("http_path", "<HTTP PATH>")
  ] %}
  {% if elementary_database %}
    {% do parameters.append(_parameter("catalog", elementary_database)) %}
  {% endif %}
  {% do parameters.extend([
    _parameter("schema", elementary_schema),
    _parameter("token", "<TOKEN>"),
    _parameter("threads", target.threads),
  ]) %}
  {% do return(parameters) %}
{% endmacro %}

{% macro athena__generate_elementary_profile_args(method, elementary_database, elementary_schema) %}
  {% set parameters = [
    _parameter("type", target.type),
    _parameter("s3_staging_dir", target.s3_staging_dir),
    _parameter("region_name", target.region_name),
    _parameter("database", target.database),
    _parameter("aws_profile_name", target.aws_profile_name),
    _parameter("work_group", target.work_group),
    _parameter("aws_access_key_id", "<AWS_ACCESS_KEY_ID>"),
    _parameter("aws_secret_access_key", "<AWS_SECRET_ACCESS_KEY>"),
  ] %}

  {% if elementary_database %}
    {% do parameters.append(_parameter("catalog", elementary_database)) %}
  {% endif %}

  {% do parameters.extend([
    _parameter("schema", elementary_schema),
    _parameter("token", "<TOKEN>"),
    _parameter("threads", target.threads),
  ]) %}
  {% do return(parameters) %}
{% endmacro %}


{% macro trino__generate_elementary_profile_args(method, elementary_database, elementary_schema) %}
  {% set parameters = [
    _parameter("type", target.type),
    _parameter("host", target.host),
    _parameter("port", target.port),
    _parameter("database", elementary_database),
    _parameter("schema", elementary_schema),
    _parameter("threads", target.threads),
  ] %}

  {% if method == "ldap" %}
    {% do parameters.append(_parameter("method", "ldap")) %}
    {% do parameters.append(_parameter("user", target.user)) %}
    {% do parameters.append(_parameter("password", "<PASSWORD>")) %}
  {% elif method == "kerberos" %}
    {% do parameters.append(_parameter("method", "kerberos")) %}
    {% do parameters.append(_parameter("user", target.user)) %}
    {% do parameters.append(_parameter("keytab", target.keytab)) %}
    {% do parameters.append(_parameter("krb5_config", target.krb5_config)) %}
    {% do parameters.append(_parameter("principal", target.principal)) %}
  {% elif method == "jwt" %}
    {% do parameters.append(_parameter("method", "jwt")) %}
    {% do parameters.append(_parameter("jwt_token", target.jwt_token)) %}
  {% elif method == "certificate" %}
    {% do parameters.append(_parameter("method", "certificate")) %}
    {% do parameters.append(_parameter("client_certificate", target.client_certificate)) %}
    {% do parameters.append(_parameter("client_private_key", target.client_private_key)) %}
    {% do parameters.append(_parameter("cert", target.cert)) %}
  {% elif method == "oauth" %}
    {% do parameters.append(_parameter("method", "oauth")) %}
    {% do parameters.append(_parameter("user", target.user)) %}
  {% elif method == "oauth_console" %}
    {% do parameters.append(_parameter("method", "oauth_console")) %}
    {% do parameters.append(_parameter("user", target.user)) %}
  {% else %}
    {% do parameters.append(_parameter("method", "<AUTH_METHOD>", "Configure your auth method and add the required fields according to https://docs.getdbt.com/docs/core/connect-data-platform/trino-setup#authentication-parameters")) %}
  {% endif %}
  {% do return(parameters) %}
{% endmacro %}

{% macro dremio__generate_elementary_profile_args(method, elementary_database, elementary_schema) %}
  {% do return([
    _parameter("project_id", target.cloud_project_id),
    _parameter("host", target.cloud_host),
    _parameter("object_storage", elementary_database),
    _parameter("object_storage_path", elementary_schema),
    _parameter("user", target.user),
    _parameter("token", "<TOKEN>"),
    _parameter("threads", target.threads),
  ]) %}
{% endmacro %}

{% macro default__generate_elementary_profile_args(method, elementary_database, elementary_schema) %}
Adapter "{{ target.type }}" is not supported on Elementary.
{% endmacro %}
