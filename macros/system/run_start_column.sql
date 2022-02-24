{% macro run_start_column() %}
    cast ({{ run_started_at.timestamp() }} as {{ dbt_utils.type_timestamp() }} )
{% endmacro %}