{% test all_columns_anomalies(model, column_tests) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_data_monitoring') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
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
        -- TODO: not sure this works
        {%- set model_relation = dbt.load_relation(model) %}
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}
        -- TODO: see if we need to change the query to a new final_table_cofig schema
        {%- set config_query = elementary.get_monitored_table_config_query(full_table_name) %}
        {%- set table_config = elementary.result_row_to_dict(config_query) %}

        {%- set timestamp_column = elementary.insensitive_get_dict_value(table_config, 'timestamp_column') %}
        {%- set timestamp_column_data_type = elementary.insensitive_get_dict_value(table_config, 'timestamp_column_data_type') %}
        {%- set is_timestamp = elementary.get_is_column_timestamp(full_table_name, timestamp_column, timestamp_column_data_type) %}
        {%- set min_bucket_start = "'" ~ get_min_bucket_start(full_table_name, column_tests) ~ "'" %}

        {%- set columns_config = get_all_columns_monitors(model, column_tests) -%}

        {#- execute table monitors and write to temp test table -#}
        {%- if columns_config | length > 0 %}
            {%- for column in columns_config %}
                {%- set column_name = column['column_name'] %}
                {%- set column_monitors = column['monitors'] %}
                {%- set column_monitoring_query = elementary.column_monitoring_query(full_table_name, timestamp_column, is_timestamp, min_bucket_start, column_name, column_monitors) %}
                {%- if loop.first %}
                    {%- do run_query(dbt.create_table_as(True, temp_table_relation, column_monitoring_query)) %}
                {%- endif %}
                    {%- set temp_table_name = elementary.relation_to_full_name(temp_table_relation) %}
                    {%- do elementary.insert_as_select(temp_table_name, column_monitoring_query) -%}
            {%- endfor %}
        {%- endif %}

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
        --TODO: if exists should we drop or the following line will run create or replace?
        {%- do run_query(dbt.create_table_as(True, alerts_temp_table_relation, anomaly_alerts_query)) %}
        {%- set alerts_target_relation = ref('alerts_data_monitoring') %}
        {%- set dest_columns = adapter.get_columns_in_relation(alerts_target_relation) %}
        {%- set merge_sql = dbt.get_delete_insert_merge_sql(alerts_target_relation, alerts_temp_table_relation, 'alert_id', dest_columns) %}
        {%- do run_query(merge_sql) %}

        {#- return anomalies query as standart test query -#}
        select * from {{ alerts_temp_table_relation.include(database=True, schema=True, identifier=True) }}

    {%- else %}
        -- TODO: should we add a log message that no monitors were executed for this test?
        {#- test must run an sql query -#}
        {{ elementary.no_results_query() }}
    {%- endif %}
{% endtest %}
