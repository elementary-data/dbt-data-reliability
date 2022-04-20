{% macro clear_tests() %}
    -- depends_on: {{ ref('alerts_data_monitoring') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    {% if execute %}
        -- TODO: change to truncate
        {% set clear_alerts_tables_query %}
            DELETE FROM {{ ref('alerts_data_monitoring') }} where TRUE
        {% endset %}
        {% do run_query(clear_alerts_tables_query) %}
        {% do elementary.edr_log("cleared alerts tables") %}

        {% set clear_metrics_tables_query %}
            DELETE FROM {{ ref('data_monitoring_metrics') }} where TRUE
        {% endset %}
        {% do run_query(clear_metrics_tables_query) %}
        {% do elementary.edr_log("cleared metrics tables") %}

        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set schema_name = schema_name ~ '__tests' %}
        {% set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}
        {% do adapter.drop_schema(schema_relation) %}
        {% do elementary.edr_log("dropped schema " ~ database_name  ~ "." ~ schema_name) %}
        {% do adapter.commit() %}

    {% endif %}
    {{ return('') }}
{% endmacro %}
