{% macro print_elementary_cli_profile() %}
  {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
  {{ log('\n' ~ adapter.dispatch('print_elementary_cli_profile')(elementary_database, elementary_schema), info=True) }}
{% endmacro %}

{% macro snowflake__print_elementary_cli_profile(elementary_database, elementary_schema) %}
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

{% macro bigquery__print_elementary_cli_profile(elementary_database, elementary_schema) %}
elementary:
  outputs:
    default:
      type: {{ target.type }}
      method: <AUTH_METHOD>
      project: {{ elementary_database }}
      dataset: {{ elementary_schema }}
      threads: {{ target.threads }}
{% endmacro %}

{% macro redshift__print_elementary_cli_profile(elementary_database, elementary_schema) %}
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

{% macro databricks__print_elementary_cli_profile(elementary_database, elementary_schema) %}
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

{% macro default__print_elementary_cli_profile() %}
Adapter "{{ target.type }}" is not supported on Elementary.
{% endmacro %}
