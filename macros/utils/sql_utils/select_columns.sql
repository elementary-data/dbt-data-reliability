{% macro select_columns(column_names, timestamp_column=none) %}
    {%- set processed_columns = [] -%}
    {%- set timestamp_column_lower = timestamp_column | lower if timestamp_column else none -%}
    
    {%- for column_name in column_names -%}
        {%- if timestamp_column and column_name | lower == timestamp_column_lower -%}
            {%- do processed_columns.append(elementary.select_timestamp_column(column_name)) -%}
        {%- else -%}
            {%- do processed_columns.append(elementary.escape_select([column_name])) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ processed_columns | join(", ") }}
{% endmacro %}
