§§{% macro get_schema_changes_test_query(full_table_name, last_schema_changes_time=none) %}
    {%- set test_execution_id = elementary.get_test_execution_id() %}
    {%- set test_unique_id = elementary.get_test_unique_id() %}

    {%- set schema_changes_test_query %}
        with column_changes as (

            select * from {{ ref('column_changes') }}
            where {{ elementary.full_table_name() }} = upper('{{ full_table_name }}')

        ),

        column_changes_test_results as (

            select
                change_id as data_issue_id,
                detected_at,
                database_name,
                schema_name,
                table_name,
                column_name,
                'schema_change' as test_type,
                change as test_sub_type,
                change_description as test_results_description
            from column_changes

        ),

        all_test_results_with_test_execution_id as (
            select {{ dbt_utils.surrogate_key([
                     'data_issue_id',
                     elementary.const_as_string(test_execution_id)
                    ]) }} as id,
                    {{ elementary.const_as_string(test_execution_id) }} as test_execution_id,
                    {{ elementary.const_as_string(test_unique_id) }} as test_unique_id,
                    *
            from column_changes_test_results
        )

        select * from all_test_results_with_test_execution_id
        {%- if last_schema_changes_time %}
            where detected_at > {{ elementary.cast_as_timestamp(elementary.const_as_string(last_schema_changes_time)) }}
        {%- endif %}
    {%- endset %}
    {{ return(schema_changes_test_query) }}
{% endmacro %}


