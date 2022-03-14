{% macro anomaly_test(metric_name) %}

    {%- set anomaly_test_query = elementary.anomaly_test_query(metric_name) %}
    {{ anomaly_test_query }}

{% endmacro %}


{% test table_anomalies(model, table_tests) %}
    -- depends_on: {{ ref('final_should_backfill') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_data_monitoring') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
    {% if execute %}

        -- TODO: create temp table for test
        {% set database_name = database %}
        {% set schema_name = target.schema ~ '__elementary_tests' %}
        {% set temp_metrics_table_name = this.name ~ '__metrics' %}
        {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_metrics_table_name,
                                                                                   type='table') -%}
        {% if not adapter.check_schema_exists(database_name, schema_name) %}
            {% do dbt.create_schema(temp_table_relation) %}
        {% endif %}

        {% set nodes = elementary.get_nodes_from_graph() %}
        {% set table_config_dict = dict() %}
        {% for node in nodes | selectattr('resource_type', 'in', 'model') -%}
            {% if node.name == model.name %}
                {% set model_edr_table_config = elementary.get_table_config(node) %}
                {% if model_edr_table_config %}
                    {% do table_config_dict.update(model_edr_table_config) %}
                {% endif %}
            {% endif %}
        {% endfor %}

        {% set full_table_name = table_config_dict['full_table_name'] %}
        {% set timestamp_column = table_config_dict['timestamp_column'] %}

        {%- set table_monitors = elementary.get_default_table_monitors() %}
        {% if table_tests %}
            {%- set table_monitors = table_tests %}
        {% endif %}

        --TODO: implement it
        {%- set is_timestamp = true %}

        --TODO: use metrics table and remove this model forever
        {%- set should_backfill_query %}
            select min_timeframe_start
            from {{ ref('final_should_backfill') }}
            where lower(full_table_name) = lower('{{ full_table_name }}')
        {%- endset %}
        {%- set timeframe_start = "'" ~ elementary.result_value(should_backfill_query) ~ "'" %}

        {%- set table_monitoring_query = elementary.table_monitoring_query(full_table_name, timestamp_column, is_timestamp, table_monitors, timeframe_start) %}
        --TODO: if exists should we drop or the following line will run create or replace?
        {% do run_query(dbt.create_table_as(True, temp_table_relation, table_monitoring_query)) %}
        -- TODO: maybe we should use adapter's merge logic?
        {% set target_relation = ref('data_monitoring_metrics') %}
        {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
        {% set merge_sql = dbt.get_delete_insert_merge_sql(target_relation, temp_table_relation, 'id', dest_columns) %}
        {% do run_query(merge_sql) %}
        -- TODO: read from big metrics table filtered on this model and metrics
        {% set anomaly_alerts_query = elementary.get_anomaly_alerts_query(full_table_name, table_monitors) %}
        {% set temp_alerts_table_name = this.name ~ '__alerts' %}
        {% set alerts_temp_table_exists, alerts_temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_alerts_table_name,
                                                                                   type='table') -%}
        -- TODO: if exists should we drop or the following line will run create or replace?
        {% do run_query(dbt.create_table_as(True, alerts_temp_table_relation, anomaly_alerts_query)) %}
        {% set alerts_target_relation = ref('alerts_data_monitoring') %}
        {% set dest_columns = adapter.get_columns_in_relation(alerts_target_relation) %}
        {% set merge_sql = dbt.get_delete_insert_merge_sql(alerts_target_relation, alerts_temp_table_relation, 'alert_id', dest_columns) %}
        {% do run_query(merge_sql) %}
        select * from {{ alerts_temp_table_relation.include(database=True, schema=True, identifier=True) }}
    {% else %}
        -- TODO: change to a query that bigquery will not hate
        select 1 as num where num = 2
    {% endif %}

{% endtest %}

{% macro column_anomalies(model, column_name, column_tests) %}
{% endmacro %}

{% macro get_monitors_empty_table_query() %}
        {% set monitors_empty_table_query = elementary.empty_table([('id','str'),
                                                                    ('full_table_name','str'),
                                                                    ('column_name','str'),
                                                                    ('metric_name','str'),
                                                                    ('metric_value','int'),
                                                                    ('timeframe_start','timestamp'),
                                                                    ('timeframe_end','timestamp'),
                                                                    ('timeframe_duration_hours','int'),
                                                                    ('description','str')]) %}
        {{ return(monitors_empty_table_query) }}
{% endmacro %}


{% macro create_temp_table(database_name, schema_name, table_name, sql_query) %}
    {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                               schema=schema_name,
                                                                               identifier=table_name,
                                                                               type='table') -%}
    {% if temp_table_exists %}
        {% do adapter.drop_relation(temp_table_relation) %}
        {% do run_query(dbt.create_table_as(True, temp_table_relation, sql_query)) %}
    {% else %}
        {% do run_query(dbt.create_table_as(True, temp_table_relation, sql_query)) %}
    {% endif %}
    {{ return(temp_table_relation) }}
{% endmacro %}


{% macro get_anomalies_empty_table_query() %}
        {% set monitors_empty_table_query = elementary.empty_table([('id','str'),
                                                                    ('full_table_name','str'),
                                                                    ('column_name','str'),
                                                                    ('metric_name','str'),
                                                                    ('z_score','int'),
                                                                    ('latest_value','int'),
                                                                    ('value_updated_at','timestamp'),
                                                                    ('metric_avg','int'),
                                                                    ('metric_stddev','int'),
                                                                    ('training_timeframe_start','timestamp'),
                                                                    ('training_timeframe_end','timestamp'),
                                                                    ('values_in_timeframe','int'),
                                                                    ('updated_at','timestamp'),
                                                                    ('description','str')]) %}
        {{ return(monitors_empty_table_query) }}
{% endmacro %}