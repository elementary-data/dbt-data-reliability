{% macro get_missing_elementary_models_err_msg() %}
    {% set elementary_db, elementary_schema = elementary.get_package_database_and_schema() %}
    {% do return("Missing Elementary models in '{}.{}'. Please run 'dbt run -s elementary --target {}'.".format(elementary_db, elementary_schema, target.name)) %}
{% endmacro %}

{% macro warn_missing_elementary_models() %}
    {% do exceptions.warn(elementary.get_missing_elementary_models_err_msg()) %}
{% endmacro %}

{% macro raise_missing_elementary_models() %}
    {% do exceptions.raise_compiler_error(elementary.get_missing_elementary_models_err_msg()) %}
{% endmacro %}
