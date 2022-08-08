{% macro print_elementary_profile() %}
  {{ log('\n\n' ~ adapter.dispatch('print_elementary_profile')() ~ '\n\n', info=True) }}
{% endmacro %}

{% macro snowflake__print_elementary_profile() -%}
elementary:
  outputs:
    default:
      type: snowflake
      account: {{ target.account }}
      user: {{ target.user }}
      password: <PASSWORD>
      role: {{ target.role }}
      database: {{ target.database }}
      warehouse: {{ target.warehouse }}
      schema: {{ target.schema }}_elementary
      threads: {{ target.threads }}
{%- endmacro %}

{% macro bigquery__print_elementary_profile() -%}
elementary:
  outputs:
    default:
      type: bigquery
      method: <AUTH_METHOD>
      project: {{ target.project }}
      dataset: {{ target.dataset }}_elementary
      threads: {{ target.threads }}
{%- endmacro %}

{% macro redshift__print_elementary_profile() -%}
elementary:
  outputs:
    default:
      type: redshift
      host: {{ target.host }}
      port: {{ target.port }}
      user: {{ target.user }}
      password: <PASSWORD>
      dbname: {{ target.database }}
      schema: {{ target.schema }}_elementary
      threads: {{ target.threads }}
{%- endmacro %}

{% macro databricks__print_elementary_profile() -%}
elementary:
  outputs:
    default:
      type: databricks
      host: {{ target.host }}
      http_path: {{ target.http_path }}
      schema: {{ target.schema }}_elementary
      token: <TOKEN>
      threads: {{ target.threads }}
{%- endmacro %}
