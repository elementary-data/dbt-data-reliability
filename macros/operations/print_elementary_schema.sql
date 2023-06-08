{% macro print_elementary_schema() %}
  {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
  {{ log('\n' ~ 'Elementary schema for target `' ~ target.name ~ '`: ' ~ elementary_database ~ '.' ~ elementary_schema ~
         ' \n' ~ adapter.dispatch('print_elementary_schema')(elementary_database, elementary_schema), info=True) }}
{% endmacro %}

{% macro snowflake__print_elementary_schema(elementary_database, elementary_schema) %}
      database: "{{ elementary_database }}"
      schema: "{{ elementary_schema }}"
{% endmacro %}

{% macro bigquery__print_elementary_schema(elementary_database, elementary_schema) %}
      project: "{{ elementary_database }}"
      dataset: "{{ elementary_schema }}"
{% endmacro %}

{% macro postgres__print_elementary_schema(elementary_database, elementary_schema) %}
      dbname: "{{ elementary_database }}"
      schema: "{{ elementary_schema }}"
{% endmacro %}

{% macro databricks__print_elementary_schema(elementary_database, elementary_schema) %}
      {%- if elementary_database %}
      catalog: "{{ elementary_database }}"
      {%- endif %}
      schema: "{{ elementary_schema }}"
{% endmacro %}

{% macro spark__print_elementary_schema(elementary_database, elementary_schema) %}
      {%- if elementary_database %}
      catalog: "{{ elementary_database }}"
      {% endif %}
      schema: "{{ elementary_schema }}"
{% endmacro %}

{% macro default__print_elementary_schema(elementary_database, elementary_schema) %}
Adapter "{{ target.type }}" is not supported on Elementary.
{% endmacro %}
