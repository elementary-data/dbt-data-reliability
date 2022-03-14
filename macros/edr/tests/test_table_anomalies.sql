{% test table_anomalies(model, table_tests) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_data_monitoring') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
    -- depends_on: {{ ref('final_tables_config') }}
    {% if execute %}
        
        {#- creates temp relation for test metrics -#}
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

        {#- get table configuration -#}
        {%- set model_relation = dbt.load_relation(model) %}
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}
        --TODO: see if we need to change the query to a new final_table_config schema
        {%- set config_query = elementary.get_monitored_table_config_query(full_table_name) %}
        {%- set table_config = elementary.result_row_to_dict(config_query) %}

        {%- set timestamp_column = elementary.insensitive_get_dict_value(table_config, 'timestamp_column') %}
        {%- set timestamp_column_data_type = elementary.insensitive_get_dict_value(table_config, 'timestamp_column_data_type') %}
        {%- set is_timestamp = elementary.get_is_column_timestamp(full_table_name, timestamp_column, timestamp_column_data_type) %}
        {%- set min_bucket_start = "'" ~ elementary.get_min_bucket_start(full_table_name,table_monitors) ~ "'" %}
        {%- set table_monitors = elementary.get_final_table_monitors(table_tests) %}

        {#- execute table monitors and write to temp test table -#}
        {{ elementary.test_log('start', full_table_name) }}
        {%- set table_monitoring_query = elementary.table_monitoring_query(full_table_name, timestamp_column, is_timestamp, min_bucket_start, table_monitors) %}
        {%- do run_query(dbt.create_table_as(False, temp_table_relation, table_monitoring_query)) %}

        {#- query if there is an anomaly in recent metrics -#}
        {%- set temp_table_name = elementary.relation_to_full_name(temp_table_relation) %}
        {% set anomaly_query = elementary.get_anomaly_query(temp_table_name, full_table_name, table_monitors) %}
        {% set temp_alerts_table_name = this.name ~ '__anomalies' %}
        {% set anomalies_temp_table_exists, anomalies_temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_alerts_table_name,
                                                                                   type='table') -%}
        {% do run_query(dbt.create_table_as(True, anomalies_temp_table_relation, anomaly_query)) %}
        {{ elementary.test_log('end', full_table_name) }}

        {# return anomalies query as standart test query #}
        select * from {{ anomalies_temp_table_relation.include(database=True, schema=True, identifier=True) }}

    {% else %}

        {# test must run an sql query #}
        {{ elementary.test_log('no_monitors', full_table_name) }}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}


  

