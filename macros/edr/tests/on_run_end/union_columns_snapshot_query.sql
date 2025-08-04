{% macro union_columns_snapshot_query(temp_columns_snapshot_tables) %}
    {%- if temp_columns_snapshot_tables | length > 0 %}
        {%- set union_temp_query -%}
            with union_temp_columns_snapshot as (
            {%- for temp_table in temp_columns_snapshot_tables -%}
                select * from {{ temp_table }}
                {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
            ),
            columns_snapshot_with_duplicates as (
                select *,
                    row_number() over (partition by column_state_id order by detected_at desc) as row_num
                from union_temp_columns_snapshot
            )
            select
                column_state_id,
                full_column_name,
                full_table_name,
                column_name,
                data_type,
                is_new,
                detected_at
            from columns_snapshot_with_duplicates
            where row_num = 1
        {%- endset %}
        {{ return(union_temp_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}
