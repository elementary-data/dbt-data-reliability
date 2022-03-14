{% macro get_schema_changes_alert_query(full_table_name, last_alert=none) %}
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
                change_id as alert_id,
                detected_at,
                database_name,
                schema_name,
                table_name,
                {{ elementary.null_string() }} as column_name,
                'schema_change' as alert_type,
                change as sub_type,
                change_description as alert_description
            from table_changes

        ),

        column_changes_alerts as (

            select
                change_id as alert_id,
                detected_at,
                database_name,
                schema_name,
                table_name,
                column_name,
                'schema_change' as alert_type,
                change as sub_type,
                change_description as alert_description
            from column_changes

        ),

        all_alerts as (

            select * from table_changes_alerts
            union all
            select * from column_changes_alerts

        )

        select * from all_alerts
        {%- if last_alert %}
            {%- set last_alert_quoted = "'"~ last_alert ~"'" %}
            where detected_at > {{ elementary.cast_as_timestamp(last_alert_quoted) }}
        {%- endif %}
    {%- endset %}
    {{ return(schema_changes_test_query) }}
{% endmacro %}


