{% macro schema_exists(database, schema) %}
  {% do return(adapter.dispatch("schema_exists", "elementary")(database, schema)) %}
{% endmacro %}

{% macro snowflake__schema_exists(database, schema) %}
  {% set database_sql %}
    show databases like '{{ database }}'
  {% endset %}
  {% set database_exists = elementary.run_query(database_sql)|length > 0 %}
  {% if not database_exists %}
    {% do return(false) %}
  {% endif %}
  {% do return(adapter.check_schema_exists(database, schema)) %}
{% endmacro %}

{% macro postgres__schema_exists(database, schema) %}
  {% if database != target.database %}
    {# Cross db operations not supported in postgres #}
    {% do return(false) %}
  {% endif %}
  {% do return(adapter.check_schema_exists(database, schema)) %}
{% endmacro %}

{% macro bigquery__schema_exists(database, schema) %}
  {% if database != target.project %}
    {# Cannot check for non-existing database in bigquery through sql (only api), assume it exists #}
    {% do return(true) %}
  {% endif %}
  {% do return(adapter.check_schema_exists(database, schema)) %}
{% endmacro %}

{% macro default__schema_exists(database, schema) %}
  {% do return(adapter.check_schema_exists(database, schema)) %}
{% endmacro %}
