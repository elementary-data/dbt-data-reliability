{% macro union_metrics_query(temp_metrics_tables) %}
    {%- if temp_metrics_tables | length > 0 %}
        {%- set union_temp_query -%}
            with union_temps_metrics as (
            {%- for temp_table in temp_metrics_tables -%}
                select * from {{ temp_table }}
                {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
            ),
            metrics_with_duplicates as (
                select *,
                    row_number() over (partition by id order by updated_at desc) as row_num
                from union_temps_metrics
            )
            select
                id,
                full_table_name,
                column_name,
                metric_name,
                metric_type,
                metric_value,
                source_value,
                bucket_start,
                bucket_end,
                bucket_duration_hours,
                updated_at,
                dimension,
                dimension_value,
                metric_properties
            from metrics_with_duplicates
            where row_num = 1
        {%- endset %}
        {{ return(union_temp_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}
