{% macro generate_elementary_cli_profile(method=none) %}
  {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
  {{ log('\n' ~ adapter.dispatch('generate_elementary_cli_profile')(method, elementary_database, elementary_schema), info=True) }}
{% endmacro %}

{% macro snowflake__generate_elementary_cli_profile(method, elementary_database, elementary_schema) %}
elementary:
  outputs:
    default:
      type: {{ target.type }}
      account: {{ target.account }}
      user: {{ target.user }}
      password: <PASSWORD>
      role: {{ target.role }}
      warehouse: {{ target.warehouse }}
      database: {{ elementary_database }}
      schema: {{ elementary_schema }}
      threads: {{ target.threads }}
{% endmacro %}

{% macro bigquery__generate_elementary_cli_profile(method, elementary_database, elementary_schema) %}
elementary:
  outputs:
    default:
      type: {{ target.type }}
      method: <AUTH_METHOD>
      project: {{ elementary_database }}
      {%- if method == 'github-actions' %}
      keyfile: /tmp/bigquery_keyfile.json # Do not change this, supply `bigquery-keyfile` in `.github/workflows/elementary.yml`.
      {%- endif %}
      dataset: {{ elementary_schema }}
      threads: {{ target.threads }}
{% endmacro %}

{% macro redshift__generate_elementary_cli_profile(method, elementary_database, elementary_schema) %}
elementary:
  outputs:
    default:
      type: {{ target.type }}
      host: {{ target.host }}
      port: {{ target.port }}
      user: {{ target.user }}
      password: <PASSWORD>
      dbname: {{ elementary_database }}
      schema: {{ elementary_schema }}
      threads: {{ target.threads }}
{% endmacro %}

{% macro databricks__generate_elementary_cli_profile(method, elementary_database, elementary_schema) %}
elementary:
  outputs:
    default:
      type: {{ target.type }}
      host: {{ target.host }}
      http_path: {{ target.http_path }}
      {%- if elementary_database %}
      catalog: {{ elementary_database }}
      {% endif %}
      schema: {{ elementary_schema }}
      token: <TOKEN>
      threads: {{ target.threads }}
{% endmacro %}

{% macro spark__generate_elementary_cli_profile(method, elementary_database, elementary_schema) %}
elementary:
  outputs:
    default:
      type: databricks
      host: {{ target.host }}
      http_path: <HTTP PATH>
      {%- if elementary_database %}
      catalog: {{ elementary_database }}
      {% endif %}
      schema: {{ elementary_schema }}
      token: <TOKEN>
      threads: {{ target.threads }}
{% endmacro %}

{% macro default__generate_elementary_cli_profile(method, elementary_database, elementary_schema) %}
Adapter "{{ target.type }}" is not supported on Elementary.
{% endmacro %}
