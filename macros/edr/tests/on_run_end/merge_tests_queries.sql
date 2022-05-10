{% macro get_temp_tables_from_run_results(results, database_name, schema_name) %}
    {% set temp_metrics_tables = [] %}
    {% set temp_anomalies_tables = [] %}
    {% set temp_schema_changes_tables = [] %}
    {% if execute %}
        {% set schema_name = schema_name ~ '__tests' %}
        {{ elementary.debug_log('finding test temp tables in database: ' ~ database_name ~ ' and schema: ' ~ schema_name) }}
        {{ elementary.debug_log('iterating over test nodes') }}
        {% for result in results %}
            {% set run_result_dict = result.to_dict() %}
            {% set node = elementary.safe_get_with_default(run_result_dict, 'node', {}) %}
            {% set status = run_result_dict.get('status') | lower %}
            {% set resource_type = node.get('resource_type') %}
            {% if resource_type == 'test' and status != 'error' %}
                {% set test_node = node %}
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
            with union_temps_metrics as (
            {%- for temp_table in temp_metrics_tables -%}
                select * from {{- temp_table -}}
                {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
            ),
            metrics_with_duplicates as (
                select *,
                    row_number() over (partition by id order by updated_at desc) as row_number
                from union_temps_metrics
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


{% macro union_anomalies_alerts_query(temp_anomalies_tables) %}
    -- depends_on: {{ source('elementary_dbt_artifacts', 'dbt_tests') }}
    {%- if temp_anomalies_tables | length > 0 %}
        {%- set dbt_tests_exists = elementary.relation_exists(source('elementary_dbt_artifacts', 'dbt_tests')) -%}
        {%- set anomalies_alerts_query %}
            with union_temp as (
                {{ elementary.union_anomalies_query(temp_anomalies_tables) }}
            ),
            data_anomalies as (
                select
                    id as alert_id,
                    metric_id as data_issue_id,
                    test_execution_id,
                    test_unique_id,
                    {{ elementary.current_timestamp_column() }} as detected_at,
                    {{ elementary.full_name_split('database_name') }},
                    {{ elementary.full_name_split('schema_name') }},
                    {{ elementary.full_name_split('table_name') }},
                    column_name,
                    'anomaly_detection' as alert_type,
                    metric_name as sub_type,
                    anomaly_description as alert_description,
                    anomalous_value as other,
                    row_number() over (partition by id order by column_name) as row_index
                from union_temp
            ),
            data_anomalies_no_dups as (
                select * from data_anomalies where row_index = 1
            ),
            data_anomalies_enriched_with_operational_context as (
                select
                    da.alert_id,
                    da.data_issue_id,
                    da.test_execution_id,
                    da.detected_at,
                    da.database_name,
                    da.schema_name,
                    da.table_name,
                    da.column_name,
                    da.alert_type,
                    da.sub_type,
                    da.alert_description,
                {%- if dbt_tests_exists -%}
                    dt.model_owners as owners,
                    dt.model_tags as tags,
                    dt.compiled_sql as alert_results_query,
                    da.other,
                    dt.short_name as test_name,
                    dt.test_params,
                    dt.severity,
                    'fail' as status
                from data_anomalies_no_dups da join {{ source('elementary_dbt_artifacts', 'dbt_tests') }} dt on da.test_unique_id = dt.unique_id
                {%- else -%}
                    {{ elementary.null_string() }} as owners,
                    {{ elementary.null_string() }} as tags,
                    {{ elementary.null_string() }} as alert_results_query,
                    da.other,
                    {{ elementary.null_string() }} as test_name,
                    {{ elementary.null_string() }} as test_params,
                    {{ elementary.null_string() }} as severity,
                    'fail' as status
                from data_anomalies_no_dups da
                {%- endif -%}
            )
            select * from data_anomalies_enriched_with_operational_context
        {%- endset %}
        {{ return(anomalies_alerts_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}


{% macro union_schema_changes_query(temp_schema_changes_tables) %}
    -- depends_on: {{ source('elementary_dbt_artifacts', 'dbt_tests') }}
    {%- if temp_schema_changes_tables | length > 0 %}
        {%- set dbt_tests_exists = elementary.relation_exists(source('elementary_dbt_artifacts', 'dbt_tests')) -%}
        {%- set union_temp_query -%}
            with schema_changes as (
            {%- for temp_table in temp_schema_changes_tables -%}
                select * from {{- temp_table -}}
                {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
            ),
            schema_changes_with_indices as (
                select *,
                      row_number() over (partition by alert_id order by detected_at desc) as row_index
                from schema_changes
            ),
            schema_changes_no_dups as (
                select * from schema_changes_with_indices where row_index = 1
            ),
            schema_changes_enriched_with_operational_context as (
                select
                    sc.alert_id,
                    sc.data_issue_id,
                    sc.test_execution_id,
                    sc.detected_at,
                    sc.database_name,
                    sc.schema_name,
                    sc.table_name,
                    sc.column_name,
                    sc.alert_type,
                    sc.sub_type,
                    sc.alert_description,
                {%- if dbt_tests_exists -%}
                    dt.model_owners as owners,
                    dt.model_tags as tags,
                    dt.compiled_sql as alert_results_query,
                    sc.other,
                    dt.short_name as test_name,
                    dt.test_params,
                    dt.severity,
                    'fail' as status
                from schema_changes_no_dups sc join {{ source('elementary_dbt_artifacts', 'dbt_tests') }} dt on sc.test_unique_id = dt.unique_id
                {%- else -%}
                    {{ elementary.null_string() }} as owners,
                    {{ elementary.null_string() }} as tags,
                    {{ elementary.null_string() }} as alert_results_query,
                    sc.other,
                    {{ elementary.null_string() }} as test_name,
                    {{ elementary.null_string() }} as test_params,
                    {{ elementary.null_string() }} as severity,
                    'fail' as status
                from schema_changes_no_dups sc
                {% endif %}
            )
            select * from schema_changes_enriched_with_operational_context
        {%- endset %}
        {{ return(union_temp_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}