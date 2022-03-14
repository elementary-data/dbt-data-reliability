{% test schema_changes(model) %}
    -- depends_on: {{ ref('alerts_schema_changes') }}
    -- depends_on: {{ ref('table_changes') }}
    -- depends_on: {{ ref('column_changes') }}
    -- depends_on: {{ ref('final_tables_config') }}
    {% if execute %}

        {# creates temp relation for test metrics #}
        {% set database_name = database %}
        {% set schema_name = target.schema ~ '__elementary_tests' %}
        {% set temp_metrics_table_name = this.name ~ '__schema_changes' %}
        {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_metrics_table_name,
                                                                                   type='table') -%}
        {% if not adapter.check_schema_exists(database_name, schema_name) %}
            {% do dbt.create_schema(temp_table_relation) %}
        {% endif %}

        {# get table configuration #}
        {%- set model_relation = dbt.load_relation(model) %}
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}
        {%- set last_schema_change_alert_time = elementary.get_last_schema_changes_alert_time() %}

        {# query if there were schema changes since last execution #}
        {% set schema_changes_alert_query = elementary.get_schema_changes_alert_query(full_table_name, last_schema_change_alert_time) %}
        {% set temp_alerts_table_name = this.name ~ '__alerts' %}
        {% set alerts_temp_table_exists, alerts_temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_alerts_table_name,
                                                                                   type='table') -%}
        {% do run_query(dbt.create_table_as(False, alerts_temp_table_relation, anomaly_alerts_query)) %}
            -- TODO: do the merge only if there are results
        {% set alerts_target_relation = ref('alerts_schema_changes') %}
        {% set dest_columns = adapter.get_columns_in_relation(alerts_target_relation) %}
        {% set merge_sql = dbt.get_delete_insert_merge_sql(alerts_target_relation, alerts_temp_table_relation, 'alert_id', dest_columns) %}
        {% do run_query(merge_sql) %}

        {# return schema changes query as standart test query #}
        select * from {{ alerts_temp_table_relation.include(database=True, schema=True, identifier=True) }}

    {% else %}

        {# test must run an sql query #}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}