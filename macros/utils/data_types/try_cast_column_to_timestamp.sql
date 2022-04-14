{% macro try_cast_column_to_timestamp(table_relation, timestamp_column) %}

    {# We only try casting for snowflake and bigquery, as these support safe cast and the query will not fail if the cast fails #}
    {%- if target.type in ['snowflake','bigquery'] %}
        {%- set query %}
            select {{ dbt_utils.safe_cast(timestamp_column, dbt_utils.type_timestamp()) }} as timestamp_column
            from {{ table_relation }}
            where {{ timestamp_column }} is not null
            limit 1
        {%- endset %}

        {%- set result = elementary.result_value(query) %}
        {%- if result is not none %}
            {{ return(true) }}
        {%- endif %}
    {%- endif %}
    {{ return(false) }}

{% endmacro %}
