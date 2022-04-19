{% macro merge_test_results() %}
    {% if execute and flags.WHICH == 'test' %}
        {% set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
        {% set temp_metrics_tables, temp_anomalies_tables, temp_schema_changes_tables = elementary.get_temp_tables_from_graph(database_name, schema_name) %}
        {{ elementary.merge_test_tables_to_target(database_name, schema_name, temp_metrics_tables, 'data_monitoring_metrics') }}
        {{ elementary.merge_test_tables_to_target(database_name, schema_name, temp_anomalies_tables, 'alerts_data_monitoring') }}
        {{ elementary.merge_test_tables_to_target(database_name, schema_name, temp_schema_changes_tables, 'alerts_schema_changes') }}
    {% endif %}
    {{ return('') }}
{% endmacro %}


{% macro merge_test_tables_to_target(database_name, schema_name, test_tables_list, target_name) %}

    {%- if test_tables_list | length > 0 %}
        {%- set test_tables_union_query = elementary.get_test_tables_union_query(test_tables_list, target_name) %}
        {%- set target_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier=target_name) %}
        {%- set temp_relation = dbt.make_temp_relation(target_relation) -%}

        {%- if test_tables_union_query %}
            {{ elementary.debug_log('Running union query from test tables to ' ~ temp_relation.identifier) }}
            {%- do run_query(dbt.create_table_as(True, temp_relation, test_tables_union_query)) %}
            {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
            {{ elementary.debug_log('Merging ' ~ temp_relation.identifier ~ ' to ' ~ target_relation.database ~ '.' ~ target_relation.schema ~ '.' ~ target_relation.identifier) }}
            {% set target_unique_id = elementary.get_target_unique_id(target_name) %}
            {% set merge_sql = elementary.merge_sql(target_relation, temp_relation, target_unique_id, dest_columns) %}
            {%- if target_relation and temp_relation and target_unique_id and dest_columns and merge_sql %}
                {% do run_query(merge_sql) %}
                {%- do adapter.commit() -%}
                {{ elementary.debug_log('Finished merging') }}
            {%- else %}
                {{ elementary.debug_log('Error: could not merge to table: ' ~ target_name) }}
            {%- endif %}
        {%- endif %}

    {%- endif %}
{% endmacro %}


{% macro get_test_tables_union_query(test_tables_list, target_name) %}
    {% set test_tables_union_query = none %}
    {%- if target_name == 'data_monitoring_metrics' %}
        {%- set test_tables_union_query = elementary.union_metrics_query(test_tables_list) %}
    {%- elif target_name == 'alerts_data_monitoring' %}
        {%- set test_tables_union_query = elementary.union_anomalies_alerts_query(test_tables_list) %}
    {%- elif target_name == 'alerts_schema_changes' %}
        {%- set test_tables_union_query = elementary.union_schema_changes_query(test_tables_list) %}
    {%- else %}
        {% do elementary.edr_log('Error: invalid target name (get_test_tables_union_query)') %}
    {%- endif %}
    {{ return(test_tables_union_query) }}
{% endmacro %}


{% macro get_target_unique_id(target_name) %}
    {% set target_unique_id = none %}
    {%- if target_name == 'data_monitoring_metrics' %}
        {%- set target_unique_id = 'id' %}
    {%- elif target_name == 'alerts_data_monitoring' %}
        {%- set target_unique_id = 'alert_id' %}
    {%- elif target_name == 'alerts_schema_changes' %}
        {%- set target_unique_id = 'alert_id' %}
    {%- else %}
        {% do elementary.edr_log('Error: invalid target name (get_target_unique_id)') %}
    {%- endif %}
    {{ return(target_unique_id) }}
{% endmacro %}

