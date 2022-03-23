{% test schema_changes(model) %}
    -- depends_on: {{ ref('alerts_schema_changes') }}
    -- depends_on: {{ ref('table_changes') }}
    -- depends_on: {{ ref('column_changes') }}
    {% if execute %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ this.name) }}
        {# creates temp relation for test metrics #}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set schema_name = schema_name ~ '__tests' %}
        {% set temp_schema_changes_table_name = this.name ~ '__schema_changes' %}
        {{ elementary.debug_log('metrics table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_schema_changes_table_name) }}
        {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_schema_changes_table_name,
                                                                                   type='table') -%}
        {% if not adapter.check_schema_exists(database_name, schema_name) %}
            {{ elementary.debug_log('schema ' ~ database_name ~ '.' ~ schema_name ~ ' doesnt exist, creating it') }}
            {% do dbt.create_schema(temp_table_relation) %}
        {% endif %}

        {# get table configuration #}
        {%- set model_relation = dbt.load_relation(model) %}
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}
        {%- set last_schema_change_alert_time = elementary.get_last_schema_changes_alert_time() %}

        {# query if there were schema changes since last execution #}
        {% set schema_changes_alert_query = elementary.get_schema_changes_alert_query(full_table_name, last_schema_change_alert_time) %}
        {% set temp_alerts_table_name = this.name ~ '__schema_alerts' %}
        {{ elementary.debug_log('schema alerts table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_alerts_table_name) }}
        {% set alerts_temp_table_exists, alerts_temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_alerts_table_name,
                                                                                   type='table') -%}
        {% do run_query(dbt.create_table_as(False, alerts_temp_table_relation, schema_changes_alert_query)) %}

        {# return schema changes query as standart test query #}
        select * from {{ alerts_temp_table_relation.include(database=True, schema=True, identifier=True) }}

    {% else %}

        {# test must run an sql query #}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}