{% macro select_columns(column_names, timestamp_column=none) %}
    {%- set columns_to_select = [] -%}
    {%- set timestamp_column_lower = timestamp_column | lower if timestamp_column else none -%}
    
    {%- for column_name in column_names -%}
        {%- if timestamp_column and column_name | lower == timestamp_column_lower -%}
            {%- do columns_to_select.append(elementary.format_timestamp_column(column_name)) -%}
        {%- else -%}
            {%- do columns_to_select.append(column_name) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ elementary.escape_select(columns_to_select) }}
{% endmacro %}
