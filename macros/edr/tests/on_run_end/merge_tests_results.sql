{% macro merge_test_results() %}
    {% if execute and flags.WHICH == 'test' %}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set temp_metrics_tables, temp_anomalies_tables, temp_schema_changes_tables = elementary.get_temp_tables_from_graph(database_name, schema_name) %}
        {{ elementary.merge_temp_tables_data_to_target(database_name, schema_name, temp_metrics_tables, 'metrics', 'id') }}
        {{ elementary.merge_temp_tables_data_to_target(database_name, schema_name, temp_anomalies_tables, 'anomalies', 'alert_id') }}
        {{ elementary.merge_temp_tables_data_to_target(database_name, schema_name, temp_schema_changes_tables, 'schema_changes', 'alert_id') }}
    {% endif %}
    {{ return('') }}
{% endmacro %}


{% macro get_merge_target_and_union_query(temp_data_type, temp_tables_list) %}
    {%- if temp_data_type == 'metrics' %}
        {%- set temp_tables_union_query = elementary.union_metrics_query(temp_tables_list) %}
        {{ return(('data_monitoring_metrics', temp_tables_union_query)) }}
    {%- elif temp_data_type == 'anomalies' %}
        {%- set temp_tables_union_query = elementary.union_anomalies_alerts_query(temp_tables_list) %}
        {{ return(('alerts_data_monitoring', temp_tables_union_query)) }}
    {%- elif temp_data_type == 'schema_changes' %}
        {%- set temp_tables_union_query = elementary.union_schema_changes_query(temp_tables_list) %}
        {{ return(('alerts_schema_changes',temp_tables_union_query)) }}
    {%- else %}
        {% do elementary.edr_log('Error: could not find merge target table') %}
        {{ return('') }}
    {%- endif %}
{% endmacro %}


{% macro merge_temp_tables_data_to_target(database_name, schema_name, temp_tables_list, temp_data_type, unique_id) %}

    {%- if temp_tables_list | length >0 %}
        {%- set target_table_name, temp_tables_union_query = elementary.get_merge_target_and_union_query(temp_data_type, temp_tables_list) %}
        {%- set target_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier=target_table_name) %}
        {%- set temp_relation = dbt.make_temp_relation(target_relation) -%}

        {%- if temp_tables_union_query %}
            {{ elementary.debug_log('pulling ' ~ temp_data_type ~ ' from temp tables to: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_relation.identifier) }}
            {%- do run_query(dbt.create_table_as(True, temp_relation, temp_tables_union_query)) %}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
            {{ elementary.debug_log('merging ' ~ temp_data_type ~ ' to: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ target_table_name) }}
            {% set merge_sql = elementary.merge_sql(target_relation, temp_relation, unique_id, dest_columns) %}
            {% do run_query(merge_sql) %}
            {%- do adapter.commit() -%}
            {{ elementary.debug_log('finished merging ' ~ temp_data_type) }}
        {%- endif %}

    {%- endif %}
{% endmacro %}
