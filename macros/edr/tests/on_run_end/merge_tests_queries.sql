{% macro get_temp_tables_from_graph(database_name, schema_name) %}
    {% set temp_metrics_tables = [] %}
    {% set temp_anomalies_tables = [] %}
    {% set temp_schema_changes_tables = [] %}
    {% if execute %}
        {% set schema_name = schema_name ~ '__tests' %}
        {{ elementary.debug_log('finding test temp tables in database: ' ~ database_name ~ ' and schema: ' ~ schema_name) }}
        {% set test_nodes = elementary.get_nodes_from_graph() | selectattr('resource_type', '==', 'test') %}
        {{ elementary.debug_log('iterating over test nodes') }}
        {% for test_node in test_nodes %}
            {% set test_metadata = test_node.get('test_metadata') %}
            {% if test_metadata %}
                {% set test_name = test_metadata.get('name') %}
                {% if test_name in ['table_anomalies', 'column_anomalies', 'all_columns_anomalies'] %}
                    {% set temp_metrics_table_name = test_node.name ~ '__metrics' %}
                    {% set temp_metrics_table_relation = adapter.get_relation(database=database_name,
                                                                              schema=schema_name,
                                                                              identifier=temp_metrics_table_name) %}
                    {% if temp_metrics_table_relation %}
                        {% set full_metrics_table_name = temp_metrics_table_relation.render() %}
                        {% do temp_metrics_tables.append(full_metrics_table_name) %}
                    {% endif %}

                    {% set temp_anomalies_table_name = test_node.name ~ '__anomalies' %}
                    {% set temp_anomalies_table_relation = adapter.get_relation(database=database_name,
                                                                                schema=schema_name,
                                                                                identifier=temp_anomalies_table_name) %}
                    {% if temp_anomalies_table_relation %}
                        {% set full_anomalies_table_name = temp_anomalies_table_relation.render() %}
                        {% do temp_anomalies_tables.append(full_anomalies_table_name) %}
                    {% endif %}
                {% elif test_name == 'schema_changes' %}
                    {% set test_schema_changes_table_name = test_node.name ~ '__schema_changes_alerts' %}
                    {% set test_schema_changes_table_relation = adapter.get_relation(database=database_name,
                                                                                     schema=schema_name,
                                                                                     identifier=test_schema_changes_table_name) %}
                    {% if test_schema_changes_table_relation %}
                        {% set full_schema_changes_table_name = test_schema_changes_table_relation.render() %}
                        {% do temp_schema_changes_tables.append(full_schema_changes_table_name) %}
                    {% endif %}
                {% endif %}
            {% endif %}
        {% endfor %}
        {{ elementary.debug_log('found temp_metrics_tables: ' ~ temp_metrics_tables) }}
        {{ elementary.debug_log('found temp_anomalies_tables: ' ~ temp_anomalies_tables) }}
        {{ elementary.debug_log('found temp_schema_changes_tables: ' ~ temp_schema_changes_tables) }}
    {% endif %}
    {{ return([temp_metrics_tables, temp_anomalies_tables, temp_schema_changes_tables]) }}
{% endmacro %}


{% macro union_anomalies_query(temp_anomalies_tables) %}
    {%- if temp_anomalies_tables | length > 0 %}
        {%- set union_temp_query -%}
            {%- for temp_table in temp_anomalies_tables -%}
                select * from {{ temp_table }}
                {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
        {%- endset %}
        {{ return(union_temp_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}


{% macro union_metrics_query(temp_metrics_tables) %}
    {%- if temp_metrics_tables | length > 0 %}
        {%- set union_temp_query -%}
            with union_temps as (
            {%- for temp_table in temp_metrics_tables -%}
                select * from {{- temp_table -}}
                {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
            ),
            metrics_with_duplicates as (
                select *,
                    row_number() over (partition by id order by updated_at desc) as row_number
                from union_temps
            )
            select
                id,
                full_table_name,
                column_name,
                metric_name,
                metric_value,
                source_value,
                bucket_start,
                bucket_end,
                bucket_duration_hours,
                updated_at
            from metrics_with_duplicates
            where row_number = 1
        {%- endset %}
        {{ return(union_temp_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}


{% macro anomalies_alerts_query(temp_anomalies_tables) %}
    {%- if temp_anomalies_tables | length > 0 %}
        {%- set anomalies_alerts_query %}
            with union_temp as (
                {{ elementary.union_anomalies_query(temp_anomalies_tables) }}
            ),
            union_with_duplicates as (
                select
                    id as alert_id,
                    {{ elementary.current_timestamp_column() }} as detected_at,
                    {{ elementary.full_name_split('database_name') }},
                    {{ elementary.full_name_split('schema_name') }},
                    {{ elementary.full_name_split('table_name') }},
                    column_name,
                    'anomaly_detection' as alert_type,
                    metric_name as sub_type,
                    {{ elementary.anomaly_detection_description() }},
                    {{ elementary.null_string() }} as owner,
                    {{ elementary.null_string() }} as tags,
                    {{ elementary.null_string() }} as alert_results_query,
                    source_value as other,
                    row_number() over (partition by id order by column_name) as row_number
                from union_temp
            )
            select
                alert_id,
                detected_at,
                database_name,
                schema_name,
                table_name,
                column_name,
                alert_type,
                sub_type,
                alert_description,
                owner,
                tags,
                alert_results_query,
                other
            from union_with_duplicates
            where row_number = 1
        {%- endset %}
        {{ return(anomalies_alerts_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}


{% macro union_schema_changes_query(temp_schema_changes_tables) %}
    {%- if temp_schema_changes_tables | length > 0 %}
        {%- set union_temp_query -%}
            with union_temps as (
            {%- for temp_table in temp_schema_changes_tables -%}
                select * from {{- temp_table -}}
                {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
            ),
            union_with_duplicates as (
            select *,
                row_number() over (partition by alert_id order by detected_at desc) as row_number
            from union_temps
            )
            select
                alert_id,
                detected_at,
                database_name,
                schema_name,
                table_name,
                column_name,
                alert_type,
                sub_type,
                alert_description,
                owner,
                tags,
                alert_results_query,
                other
            from union_with_duplicates
            where row_number = 1
        {%- endset %}
        {{ return(union_temp_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}