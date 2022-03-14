-- TODO: old, remove after we rewrite the merge logic

{% macro anomaly_detection_merge() %}
    {%- if execute %}
        {%- set results_query = elementary.anomalies_test_results_query() %}
        {%- set unique_key = 'alert_id' %}
        {%- set get_relation = adapter.get_relation(database=elementary.target_database(),
                                                        schema=target.schema,
                                                        identifier='alerts_data_monitoring') %}
        {%- set target_relation = get_relation.incorporate(type='table') %}
        {%- set tmp_relation = make_temp_relation(target_relation) %}

        {% do run_query(dbt.create_table_as(True, tmp_relation, results_query)) %}
        {% do adapter.expand_target_column_types(
                 from_relation=tmp_relation,
                 to_relation=target_relation) %}

        {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
        {% set merge_sql = dbt.get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns) %}

        {%- do run_query(merge_sql) -%}
    {%- endif %}
    {{ return('') }}

{% endmacro %}


{% macro anomalies_test_results_query() %}
    {%- set tests_list = elementary.get_tests_list() %}
    {%- set result_schema = target.schema ~ '_dbt_test__audit' %}

    {%- set anomalies_test_results_query %}
        with all_results as (
            {%- if elementary.schema_exists_in_target(result_schema) %}
                {%- for test in tests_list %}
                    {%- set result_table = test ~ '_alerts_data_monitoring_' %}
                    select
                        id as alert_id,
                        updated_at as detected_at,
                        {{- elementary.full_name_split('database_name') -}},
                        {{- elementary.full_name_split('schema_name') -}},
                        {{- elementary.full_name_split('table_name') -}},
                        column_name,
                        'anomaly_detection' as alert_type,
                        metric_name as sub_type,
                        description
                    from {{ elementary.target_database() ~'.'~ result_schema ~'.'~ result_table }}
                         union all
                {%- endfor %}
            {%- endif %}
            ({{ elementary.empty_alerts() }})
        )
        select * from all_results
        where alert_id is not null
    {%- endset %}

    {{ return(anomalies_test_results_query) }}
{% endmacro %}



{% macro get_tests_list() %}
    {%- set tests_list = []%}

    {%- set table_monitors = var('edr_monitors')['table'] | list %}
    {%- set any_type_monitors = var('edr_monitors')['column_any_type'] | list %}
    {%- set numeric_monitors = var('edr_monitors')['column_numeric'] | list %}
    {%- set string_monitors = var('edr_monitors')['column_string'] | list %}

    {%- for monitor in table_monitors %}
        {%- if monitor != 'schema_changes' %}
            {%- set test_name = monitor ~ '_anomaly' %}
            {%- do tests_list.append(test_name) -%}
        {%- endif %}
    {%- endfor %}

    {%- for monitor in any_type_monitors %}
        {%- set test_name = monitor ~ '_anomaly' %}
        {%- do tests_list.append(test_name) -%}
    {%- endfor %}

    {%- for monitor in numeric_monitors %}
        {%- set test_name = 'numeric_' ~ monitor ~ '_anomaly' %}
        {%- do tests_list.append(test_name) -%}
    {%- endfor %}

    {%- for monitor in string_monitors %}
        {%- set test_name = 'string_' ~ monitor ~ '_anomaly' %}
        {%- do tests_list.append(test_name) -%}
    {%- endfor %}

    {{ return(tests_list) }}
{% endmacro %}