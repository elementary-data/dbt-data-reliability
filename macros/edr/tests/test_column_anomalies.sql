{% test column_anomalies(model, column_name, column_tests) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_data_monitoring') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
    -- depends_on: {{ ref('final_tables_config') }}
    {%- if execute %}

        {#- creates temp relation for test metrics -#}
        {%- set database_name = database %}
        {%- set schema_name = target.schema ~ '__elementary_tests' %}
        {%- set temp_metrics_table_name = this.name ~ '__metrics' %}
        {%- set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_metrics_table_name,
                                                                                   type='table') -%}
        {%- if not adapter.check_schema_exists(database_name, schema_name) %}
            {%- do dbt.create_schema(temp_table_relation) %}
        {%- endif %}

        {#- get column configuration -#}
        {%- set model_relation = dbt.load_relation(model) %}
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}
        -- TODO: see if we need to change the query to a new final_table_cofig schema
        {%- set config_query = elementary.get_monitored_table_config_query(full_table_name) %}
        {%- set table_config = elementary.result_row_to_dict(config_query) %}
        {%- set timestamp_column = elementary.insensitive_get_dict_value(table_config, 'timestamp_column') %}
        {%- set timestamp_column_data_type = elementary.insensitive_get_dict_value(table_config, 'timestamp_column_data_type') %}
        {%- set is_timestamp = elementary.get_is_column_timestamp(full_table_name, timestamp_column, timestamp_column_data_type) %}
        {%- set column_monitors = elementary.get_column_monitors(model, column_name, column_tests) -%}

        {%- set min_bucket_start = "'" ~ elementary.get_min_bucket_start(full_table_name, column_monitors, column_name) ~ "'" %}

        {#- execute table monitors and write to temp test table -#}
        {{ elementary.test_log('start', full_table_name, column_name) }}
        {%- set column_monitoring_query = elementary.column_monitoring_query(full_table_name, timestamp_column, is_timestamp, min_bucket_start, column_name, column_monitors) %}
        {%- do run_query(dbt.create_table_as(True, temp_table_relation, column_monitoring_query)) %}

        {#- merge results to incremental metrics table -#}
        -- TODO: maybe we should use adapter's merge logic?
        {%- set target_relation = ref('data_monitoring_metrics') %}
        {%- set dest_columns = adapter.get_columns_in_relation(target_relation) %}
        {%- set merge_sql = dbt.get_delete_insert_merge_sql(target_relation, temp_table_relation, 'id', dest_columns) %}
        {%- do run_query(merge_sql) %}

        {#- query if there is an anomaly in recent metrics -#}
        {%- set anomaly_alerts_query = elementary.get_anomaly_alerts_query(full_table_name, column_monitors, column_name) %}
        {%- set temp_alerts_table_name = this.name ~ '__alerts' %}
        {%- set alerts_temp_table_exists, alerts_temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_alerts_table_name,
                                                                                   type='table') -%}
        {%- do run_query(dbt.create_table_as(True, alerts_temp_table_relation, anomaly_alerts_query)) %}
        {%- set alerts_target_relation = ref('alerts_data_monitoring') %}
        {%- set dest_columns = adapter.get_columns_in_relation(alerts_target_relation) %}
        {%- set merge_sql = dbt.get_delete_insert_merge_sql(alerts_target_relation, alerts_temp_table_relation, 'alert_id', dest_columns) %}
        {%- do run_query(merge_sql) %}
        {{ elementary.test_log('end', full_table_name, column_name) }}

        {# return anomalies query as standart test query #}
        select * from {{ alerts_temp_table_relation.include(database=True, schema=True, identifier=True) }}
    
    {%- else %}

        {#- test must run an sql query -#}
        {{ elementary.test_log('no_monitors', full_table_name, column_name) }}
        {{ elementary.no_results_query() }}

    {%- endif %}
{% endtest %}



