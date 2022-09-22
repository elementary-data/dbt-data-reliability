{% macro get_columns_changes_query(full_table_name, temp_columns_snapshot_relation) %}

    {%- set test_execution_id = elementary.get_test_execution_id() %}
    {%- set test_unique_id = elementary.get_test_unique_id() %}
    {%- set previous_schema_time_query -%}
        (select max(detected_at) from {{ ref('schema_columns_snapshot') }} where lower(full_table_name) = lower('{{ full_table_name }}'))
    {%- endset %}

    with cur as (

        {# This is the current snapshot of the columns. #}
        select full_table_name, column_name, data_type, is_new, detected_at
        from {{ temp_columns_snapshot_relation }}

    ),

    pre as (

        {# This is the previous snapshot of the columns. #}
        select full_table_name, column_name, data_type, is_new, detected_at
        from {{ ref('schema_columns_snapshot') }}
        where lower(full_table_name) = lower('{{ full_table_name }}')
            and detected_at = {{ previous_schema_time_query }}
        order by detected_at desc

    ),

    type_changes as (

        {# Finding the columns that have changed type. #}
        select
            cur.full_table_name,
            'type_changed' as change,
            cur.column_name,
            cur.data_type as data_type,
            pre.data_type as pre_data_type,
            pre.detected_at
        from cur inner join pre
            on (cur.full_table_name = pre.full_table_name and cur.column_name = pre.column_name)
        where cur.data_type != pre.data_type

    ),

    columns_added as (

        {# This is the columns that have been added. #}
        select
            full_table_name,
            'column_added' as change,
            column_name,
            data_type,
            {{ elementary.null_string() }} as pre_data_type,
            detected_at as detected_at
        from cur
        where is_new = true

    ),

    columns_removed as (

        {# This is finding the columns that have been removed. #}
        select
            pre.full_table_name,
            'column_removed' as change,
            pre.column_name as column_name,
            {{ elementary.null_string() }} as data_type,
            pre.data_type as pre_data_type,
            pre.detected_at as detected_at
        from pre left join cur
            on (cur.full_table_name = pre.full_table_name and cur.column_name = pre.column_name)
        where cur.full_table_name is null and cur.column_name is null

    ),

    columns_removed_filter_deleted_tables as (

        {# This is filtering out the columns of tables that have been deleted. #}
        select
            removed.full_table_name,
            removed.change,
            removed.column_name,
            removed.data_type,
            removed.pre_data_type,
            removed.detected_at
        from columns_removed as removed join cur
            on (removed.full_table_name = cur.full_table_name)

    ),

    all_column_changes as (

        {# Combining the results of the three queries into one table. #}
        select * from type_changes
        union all
        select * from columns_removed_filter_deleted_tables
        union all
        select * from columns_added

    ),

    column_changes_test_results as (

        {# This is the query that is creating the test results table, by formatting a description and adding id + detection time #}
        select
            {{ dbt_utils.surrogate_key(['full_table_name', 'column_name', 'change', 'detected_at']) }} as data_issue_id,
            {{ elementary.datetime_now_utc_as_timestamp_column() }} as detected_at,
            {{ elementary.full_name_split('database_name') }},
            {{ elementary.full_name_split('schema_name') }},
            {{ elementary.full_name_split('table_name') }},
            column_name,
            'schema_change' as test_type,
            change as test_sub_type,
            case
                when change = 'column_added'
                    then 'The column "' || column_name || '" was added'
                when change= 'column_removed'
                    then 'The column "' || column_name || '" was removed'
                when change= 'type_changed'
                    then 'The type of "' || column_name || '" was changed from ' || pre_data_type || ' to ' || data_type
                else NULL
            end as test_results_description
        from all_column_changes
        {{ dbt_utils.group_by(9) }}

    )

        {# Creating a unique id for each row in the table, and adding execution id #}
    select {{ dbt_utils.surrogate_key([
                     'data_issue_id',
                     elementary.const_as_string(test_execution_id)
                ]) }} as id,
        {{ elementary.const_as_string(test_execution_id) }} as test_execution_id,
        {{ elementary.const_as_string(test_unique_id) }} as test_unique_id,
        *
    from column_changes_test_results

{%- endmacro %}