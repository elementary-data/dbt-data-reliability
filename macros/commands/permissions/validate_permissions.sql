{% macro validate_permissions() %}
    {% do return(adapter.dispatch('validate_permissions','elementary')()) %}
{% endmacro %}

{% macro bigquery__validate_permissions() %}
    {% do print("\nValidating all required permissions are granted:") %}
    {% do elementary.validate_information_schema_permissions() %}
    {% do elementary.validate_query_history_permissions() %}
    {% do print("\nAll required permissions are granted!") %}
{% endmacro %}

{% macro default__validate_permissions() %}
  {{ exceptions.raise_compiler_error("This macro is not supported on '{}'.".format(target.type)) }}
{% endmacro %}
