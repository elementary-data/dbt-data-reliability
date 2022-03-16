{% macro merge_test_results() %}
    {% if execute and flags.WHICH == 'test' %}
        {{ elementary.merge_data_monitoring_metrics() }}
        {{ elementary.merge_data_monitoring_anomalies() }}
        {{ elementary.merge_schema_changes_alerts() }}
    {% endif %}
    {{ return('') }}
{% endmacro %}


{% macro merge_data_monitoring_metrics() %}
    {%- if elementary.get_temp_tables('metrics') | length >0 %}

        {%- set temp_tables_union_query = elementary.union_metrics_query() %}

        {% set database_name = database %}
        {% set schema_name = target.schema ~ '__elementary_tests' %}
        {% set temp_metrics_table_name = 'temp_union__metrics' %}
        {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_metrics_table_name,
                                                                                   type='table') -%}
        {%- if temp_tables_union_query %}
            {%- do run_query(dbt.create_table_as(True, temp_table_relation, temp_tables_union_query)) %}

            {% set target_relation = adapter.get_relation(database=elementary.target_database(),
                                                                   schema=target.schema,
                                                                   identifier='data_monitoring_metrics') -%}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
            {% set merge_sql = dbt.get_merge_sql(target_relation, temp_table_relation, 'id', dest_columns) %}
            {% do run_query(merge_sql) %}
        {%- endif %}

    {%- endif %}
{% endmacro %}


{% macro merge_data_monitoring_anomalies() %}
    {%- if elementary.get_temp_tables('anomalies') | length >0 %}

        {%- set temp_tables_union_query = elementary.anomalies_alerts_query() %}

        {% set database_name = database %}
        {% set schema_name = target.schema ~ '__elementary_tests' %}
        {% set temp_anomalies_table_name = 'temp_union__anomalies' %}
        {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_anomalies_table_name,
                                                                                   type='table') -%}
        {%- if temp_tables_union_query %}
            {%- do run_query(dbt.create_table_as(True, temp_table_relation, temp_tables_union_query)) %}

            {% set target_relation = adapter.get_relation(database=elementary.target_database(),
                                                           schema=target.schema,
                                                           identifier='alerts_data_monitoring') -%}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
            {% set merge_sql = dbt.get_merge_sql(target_relation, temp_table_relation, 'alert_id', dest_columns) %}
            {% do run_query(merge_sql) %}
        {%- endif %}

    {%- endif %}
{% endmacro %}


{% macro merge_schema_changes_alerts() %}
    {%- if elementary.get_temp_tables('schema_alerts') | length >0 %}

        {%- set temp_tables_union_query = elementary.union_schema_changes_query() %}

        {% set database_name = database %}
        {% set schema_name = target.schema ~ '__elementary_tests' %}
        {% set temp_anomalies_table_name = 'temp_union__schema_changes_alerts' %}
        {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_anomalies_table_name,
                                                                                   type='table') -%}
        {%- if temp_tables_union_query %}
            {%- do run_query(dbt.create_table_as(True, temp_table_relation, temp_tables_union_query)) %}

            {% set target_relation = adapter.get_relation(database=elementary.target_database(),
                                                           schema=target.schema,
                                                           identifier='alerts_schema_changes') -%}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
            {% set merge_sql = dbt.get_merge_sql(target_relation, temp_table_relation, 'alert_id', dest_columns) %}
            {% do run_query(merge_sql) %}
        {%- endif %}
    {%- endif %}
{% endmacro %}
