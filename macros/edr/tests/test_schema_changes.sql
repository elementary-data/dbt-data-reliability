{% test schema_changes(model) %}
    -- depends_on: {{ ref('alerts_schema_changes') }}
    -- depends_on: {{ ref('table_changes') }}
    -- depends_on: {{ ref('column_changes') }}
    {% if execute %}
        {% set test_name_in_graph = elementary.get_test_name_in_graph() %}
        {{ elementary.debug_log('collecting metrics for test: ' ~ test_name_in_graph) }}
        {# creates temp relation for test metrics #}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set schema_name = schema_name ~ '__tests' %}
        {% set temp_schema_changes_table_name = test_name_in_graph ~ '__schema_changes' %}
        {{ elementary.debug_log('metrics table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_schema_changes_table_name) }}
        {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_schema_changes_table_name,
                                                                                   type='table') -%}
        {% if not elementary.check_schema_exists(database_name, schema_name) %}
            {{ elementary.debug_log('schema ' ~ database_name ~ '.' ~ schema_name ~ ' doesnt exist, creating it') }}
            {% do dbt.create_schema(temp_table_relation) %}
            {% do adapter.commit() %}
        {% endif %}

        {# get table configuration #}
        {%- set full_table_name = elementary.relation_to_full_name(model) %}
        {%- set model_relation = dbt.load_relation(model) %}
        {% if not model_relation %}
            {{ elementary.test_log('monitored_table_not_found', full_table_name) }}
            {{ return(elementary.no_results_query()) }}
        {% endif %}
        {%- set last_schema_change_alert_time = elementary.get_last_schema_changes_alert_time() %}

        {# query if there were schema changes since last execution #}
        {% set schema_changes_alert_query = elementary.get_schema_changes_alert_query(full_table_name, last_schema_change_alert_time) %}
        {% set temp_alerts_table_name = test_name_in_graph ~ '__schema_alerts' %}
        {{ elementary.debug_log('schema alerts table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_alerts_table_name) }}
        {% set alerts_temp_table_exists, alerts_temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_alerts_table_name,
                                                                                   type='table') -%}
        {%- do dbt.drop_relation_if_exists(alerts_temp_table_relation) %}
        {% do run_query(dbt.create_table_as(False, alerts_temp_table_relation, schema_changes_alert_query)) %}
        {% do adapter.commit() %}
        {# return schema changes query as standard test query #}
        select * from {{ alerts_temp_table_relation }}

    {% else %}

        {# test must run an sql query #}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}