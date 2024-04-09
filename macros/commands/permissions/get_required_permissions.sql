{% macro get_required_permissions() %}
    {% do return(adapter.dispatch('get_required_permissions','elementary')()) %}
{% endmacro %}

{% macro bigquery__get_required_permissions() %}
    {% do elementary.get_required_query_history_permissions() %}
    {% do elementary.get_required_information_schema_permissions() %}
{% endmacro %}

{% macro default__get_required_permissions() %}
  {{ exceptions.raise_compiler_error("This macro is not supported on '{}'.".format(target.type)) }}
{% endmacro %}
