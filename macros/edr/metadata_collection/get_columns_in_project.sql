{% macro get_columns_in_project() %}
    {% set dbt_models_relation = ref('dbt_models') %}

    {% set databases_query %}
        select
            database_name
        from {{ ref('dbt_models') }}
        group by 1
    {% endset %}

    {% set configured_databases = [] %}
    {% if execute %}
        {% do configured_databases.extend(elementary.result_column_to_list(databases_query)) %}
    {% endif %}
    {{ elementary.get_columns_by_configured_databases(configured_databases) }}
{% endmacro %}
