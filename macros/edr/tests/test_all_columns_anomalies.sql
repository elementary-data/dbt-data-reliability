{% test all_columns_anomalies(model, column_anomalies) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('alerts_data_monitoring') }}
    -- depends_on: {{ ref('metrics_anomaly_score') }}
    {%- if execute %}

        {#- creates temp relation for test metrics -#}
        {%- set database_name = elementary.target_database() %}
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
        {%- set table_config = elementary.get_table_config_from_graph(model, config) %}
        {%- set model_relation = dbt.load_relation(model) %}
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}

        {%- set timestamp_column = elementary.insensitive_get_dict_value(table_config, 'timestamp_column') %}
        {%- set timestamp_column_data_type = elementary.insensitive_get_dict_value(table_config, 'timestamp_column_data_type') %}
        {%- set is_timestamp = elementary.get_is_column_timestamp(full_table_name, timestamp_column, timestamp_column_data_type) %}
        {%- set min_bucket_start = "'" ~ elementary.get_min_bucket_start(full_table_name, column_tests) ~ "'" %}

        {%- set columns_config = elementary.get_all_columns_monitors(model_relation, column_tests) -%}

        {#- execute table monitors and write to temp test table -#}
        {%- if columns_config | length > 0 %}
            {{ elementary.test_log('start', full_table_name, 'all columns') }}
            {%- for column in columns_config %}
                {%- set column_name = column['column_name'] %}
                {%- set column_monitors = column['monitors'] %}
                {%- set column_monitoring_query = elementary.column_monitoring_query(full_table_name, timestamp_column, is_timestamp, min_bucket_start, column_name, column_monitors) %}
                {%- if loop.first %}
                    {%- do run_query(dbt.create_table_as(False, temp_table_relation, column_monitoring_query)) %}
                {%- endif %}
                    {%- set temp_table_name = elementary.relation_to_full_name(temp_table_relation) %}
                    {%- do elementary.insert_as_select(temp_table_name, column_monitoring_query) -%}
            {%- endfor %}
        {%- endif %}

        {#- query if there is an anomaly in recent metrics -#}
        {%- set temp_table_name = elementary.relation_to_full_name(temp_table_relation) %}
        {% set anomaly_query = elementary.get_anomaly_query(temp_table_name, full_table_name, columns_only=true) %}
        {% set temp_alerts_table_name = this.name ~ '__anomalies' %}
        {% set anomalies_temp_table_exists, anomalies_temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=temp_alerts_table_name,
                                                                                   type='table') -%}
        {% do run_query(dbt.create_table_as(False, anomalies_temp_table_relation, anomaly_query)) %}

        {{ elementary.test_log('end', full_table_name, 'all columns') }}

        {# return anomalies query as standart test query #}
        select * from {{ anomalies_temp_table_relation.include(database=True, schema=True, identifier=True) }}

    {%- else %}

        {{ elementary.test_log('no_monitors', full_table_name, 'all columns') }}
        {#- test must run an sql query -#}
        {{ elementary.no_results_query() }}

    {%- endif %}
{% endtest %}


