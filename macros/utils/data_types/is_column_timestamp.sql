{% macro is_column_timestamp(table_relation,timestamp_column,timestamp_column_data_type) %}
    {%- if timestamp_column_data_type == 'string' %}
        {%- set is_timestamp = elementary.try_cast_column_to_timestamp(table_relation, timestamp_column) %}
    {%- elif timestamp_column_data_type == 'timestamp' %}
        {%- set is_timestamp = true %}
    {%- else %}
        {%- set is_timestamp = false %}
    {%- endif %}
    {{ return(is_timestamp) }}
{% endmacro %}