{% macro validate_query_history_permissions() %}
    {% do return(adapter.dispatch('validate_query_history_permissions','elementary')()) %}
{% endmacro %}

{% macro bigquery__validate_query_history_permissions() %}
    {% set relevant_databases = elementary.get_relevant_databases() %}
    {% for relevant_database in relevant_databases %}
        {% do print('\nValidating access to INFORMATION_SCHEMA.JOBS for the project {} datasets - required role "roles/bigquery.resourceViewer"'.format(relevant_database)) %}
        {% set query = "select 1 from {}.region-{}.INFORMATION_SCHEMA.JOBS limit 1" .format(relevant_database, target.location)%}
        {% do elementary.run_query(query) %}
    {% endfor %}
{% endmacro %}

{% macro default__validate_query_history_permissions() %}
  {{ exceptions.raise_compiler_error("This macro is not supported on '{}'.".format(target.type)) }}
{% endmacro %}


{% macro get_required_query_history_permissions() %}
    {% do return(adapter.dispatch('get_required_query_history_permissions','elementary')()) %}
{% endmacro %}

{% macro bigquery__get_required_query_history_permissions() %}
    {% set relevant_databases = elementary.get_relevant_databases() %}
    {% do print('\nPlease make sure you provide the role "roles/bigquery.resourceViewer" to the following projects` datasets:') %}
    {% for relevant_database in relevant_databases %}
      {% do print(' - {}'.format(relevant_database)) %}
    {% endfor %}
{% endmacro %}

{% macro default__get_required_query_history_permissions() %}
  {{ exceptions.raise_compiler_error("This macro is not supported on '{}'.".format(target.type)) }}
{% endmacro %}
