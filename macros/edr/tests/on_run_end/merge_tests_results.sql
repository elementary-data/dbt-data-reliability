{% macro merge_test_results() %}
    {% if execute and flags.WHICH == 'test' %}
        {% set temp_metrics_tables, temp_anomalies_tables, temp_schema_changes_tables = elementary.get_temp_tables_from_graph() %}
        {{ elementary.merge_data_monitoring_metrics(temp_metrics_tables) }}
        {{ elementary.merge_data_monitoring_anomalies(temp_anomalies_tables) }}
        {{ elementary.merge_schema_changes_alerts(temp_schema_changes_tables) }}
    {% endif %}
    {{ return('') }}
{% endmacro %}


{% macro merge_data_monitoring_metrics(temp_metrics_tables) %}
    {%- if temp_metrics_tables | length >0 %}

        {%- set temp_tables_union_query = elementary.union_metrics_query(temp_metrics_tables) %}

        {% set database_name = database %}
        {% set schema_name = schema %}
        {% set temp_metrics_table_name = 'temp_union__metrics' %}
        {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_metrics_table_name,
                                                                                   type='table') -%}
        {%- if temp_tables_union_query %}
            {{ elementary.debug_log('pulling metrics from temp metrics tables to: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_metrics_table_name) }}
            {%- do run_query(dbt.create_table_as(True, temp_table_relation, temp_tables_union_query)) %}
            {% set target_relation = adapter.get_relation(database=database_name,
                                                                   schema=schema_name,
                                                                   identifier='data_monitoring_metrics') -%}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
            {{ elementary.debug_log('merging metrics to: ' ~ database_name ~ '.' ~ schema_name ~ '.data_monitoring_metrics') }}
            {% set merge_sql = dbt.get_merge_sql(target_relation, temp_table_relation, 'id', dest_columns) %}
            {% do run_query(merge_sql) %}
            {{ elementary.debug_log('finished merging metrics') }}
        {%- endif %}

    {%- endif %}
{% endmacro %}


{% macro merge_data_monitoring_anomalies(temp_anomalies_tables) %}
    {%- if temp_anomalies_tables | length >0 %}

        {%- set temp_tables_union_query = elementary.anomalies_alerts_query(temp_anomalies_tables) %}

        {% set database_name = database %}
        {% set schema_name = schema %}
        {% set temp_anomalies_table_name = 'temp_union__anomalies' %}
        {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_anomalies_table_name,
                                                                                   type='table') -%}
        {%- if temp_tables_union_query %}
            {{ elementary.debug_log('pulling anomalies from temp anomalies tables to: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_anomalies_table_name) }}
            {%- do run_query(dbt.create_table_as(True, temp_table_relation, temp_tables_union_query)) %}

            {% set target_relation = adapter.get_relation(database=database_name,
                                                           schema=schema_name,
                                                           identifier='alerts_data_monitoring') -%}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
            {{ elementary.debug_log('merging anomalies to: ' ~ database_name ~ '.' ~ schema_name ~ '.alerts_data_monitoring') }}
            {% set merge_sql = dbt.get_merge_sql(target_relation, temp_table_relation, 'alert_id', dest_columns) %}
            {% do run_query(merge_sql) %}
            {{ elementary.debug_log('finished merging anomalies') }}
        {%- endif %}

    {%- endif %}
{% endmacro %}


{% macro merge_schema_changes_alerts(temp_schema_changes_tables) %}
    {%- if temp_schema_changes_tables | length >0 %}

        {%- set temp_tables_union_query = elementary.union_schema_changes_query(temp_schema_changes_tables) %}

        {% set database_name = database %}
        {% set schema_name = schema %}
        {% set temp_schema_changes_table_name = 'temp_union__schema_changes_alerts' %}
        {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_schema_changes_table_name,
                                                                                   type='table') -%}
        {%- if temp_tables_union_query %}
            {{ elementary.debug_log('pulling schema changes alerts from temp schema changes tables to: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_schema_changes_table_name) }}
            {%- do run_query(dbt.create_table_as(True, temp_table_relation, temp_tables_union_query)) %}

            {% set target_relation = adapter.get_relation(database=database_name,
                                                           schema=schema_name,
                                                           identifier='alerts_schema_changes') -%}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
            {{ elementary.debug_log('merging schema changes alerts to: ' ~ database_name ~ '.' ~ schema_name ~ '.alerts_schema_changes') }}
            {% set merge_sql = dbt.get_merge_sql(target_relation, temp_table_relation, 'alert_id', dest_columns) %}
            {% do run_query(merge_sql) %}
            {{ elementary.debug_log('finished merging schema changes alerts') }}
        {%- endif %}
    {%- endif %}
{% endmacro %}
