{% macro validate_information_schema_permissions() %}
    {% do return(adapter.dispatch('validate_information_schema_permissions','elementary')()) %}
{% endmacro %}

{% macro bigquery__validate_information_schema_permissions() %}
    {% set relevant_databases = elementary.get_relevant_databases() %}
    {% for relevant_database in relevant_databases %}
        {% do print('\nValidating access to INFORMATION_SCHEMA.SCHEMATA for the project {} datasets - required role "roles/bigquery.dataViewer" / "roles/bigquery.metadataViewer"'.format(relevant_database)) %}
        {% set query = "select catalog_name, schema_name from {}.region-{}.INFORMATION_SCHEMA.SCHEMATA limit 1" .format(relevant_database, target.location)%}
        {% do print(query) %}
        {% do elementary.run_query(query) %}
    {% endfor %}
{% endmacro %}

{% macro default__validate_information_schema_permissions() %}
  {{ exceptions.raise_compiler_error("This macro is not supported on '{}'.".format(target.type)) }}
{% endmacro %}


{% macro get_required_information_schema_permissions() %}
    {% do return(adapter.dispatch('get_required_information_schema_permissions','elementary')()) %}
{% endmacro %}

{% macro bigquery__get_required_information_schema_permissions() %}
    {% set relevant_databases = elementary.get_relevant_databases() %}
    {% do print('\nPlease make sure you provide one of the following roles: "roles/bigquery.dataViewer" / "roles/bigquery.metadataViewer" to the following projects` datasets:') %}
    {% for relevant_database in relevant_databases %}
      {% do print(' - {}'.format(relevant_database)) %}
    {% endfor %}
{% endmacro %}

{% macro default__get_required_information_schema_permissions() %}
  {{ exceptions.raise_compiler_error("This macro is not supported on '{}'.".format(target.type)) }}
{% endmacro %}
