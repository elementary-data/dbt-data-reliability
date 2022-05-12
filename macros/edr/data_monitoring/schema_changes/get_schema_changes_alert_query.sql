{% macro get_schema_changes_alert_query(full_table_name, last_alert=none) %}
    {%- set test_execution_id = elementary.get_test_execution_id() %}
    {%- set test_unique_id = elementary.get_test_unique_id() %}

    {%- set schema_changes_test_query %}
        with table_changes as (

            select * from {{ ref('table_changes') }}
            where {{ elementary.full_table_name() }} = upper('{{ full_table_name }}')

        ),

        column_changes as (

            select * from {{ ref('column_changes') }}
            where {{ elementary.full_table_name() }} = upper('{{ full_table_name }}')

        ),

        table_changes_alerts as (

            select
                change_id as data_issue_id,
                detected_at,
                database_name,
                schema_name,
                table_name,
                {{ elementary.null_string() }} as column_name,
                'schema_change' as alert_type,
                change as sub_type,
                change_description as alert_description,
                {{ elementary.null_string() }} as owner,
                {{ elementary.null_string() }} as tags,
                {{ elementary.null_string() }} as alert_results_query,
                {{ elementary.null_string() }} as other
            from table_changes

        ),

        column_changes_alerts as (

            select
                change_id as data_issue_id,
                detected_at,
                database_name,
                schema_name,
                table_name,
                column_name,
                'schema_change' as alert_type,
                change as sub_type,
                change_description as alert_description,
                {{ elementary.null_string() }} as owner,
                {{ elementary.null_string() }} as tags,
                {{ elementary.null_string() }} as alert_results_query,
                {{ elementary.null_string() }} as other
            from column_changes

        ),

        all_alerts as (

            select * from table_changes_alerts
            union all
            select * from column_changes_alerts

        ),

        all_alerts_with_test_execution_id as (
            select {{ dbt_utils.surrogate_key([
                     'data_issue_id',
                     elementary.const_as_string(test_execution_id)
                    ]) }} as alert_id,
                    {{ elementary.const_as_string(test_execution_id) }} as test_execution_id,
                    {{ elementary.const_as_string(test_unique_id) }} as test_unique_id,
                    *
            from all_alerts
        )

        select * from all_alerts_with_test_execution_id
        {%- if last_alert %}
            {%- set last_alert_quoted = "'"~ last_alert ~"'" %}
            where detected_at > {{ elementary.cast_as_timestamp(last_alert_quoted) }}
        {%- endif %}
    {%- endset %}
    {{ return(schema_changes_test_query) }}
{% endmacro %}


