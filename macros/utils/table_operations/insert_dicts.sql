{% macro insert_dicts(table_name, dicts, chunk_size=50) %}
    {% set dicts_chunks = elementary.split_list_to_chunks(dicts, chunk_size) %}
    {% for dicts_chunk in dicts_chunks %}
        {% set insert_dicts_query = elementary.get_insert_dicts_query(table_name, dicts_chunk) %}
        {% do run_query(insert_dicts_query) %}
    {% endfor %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro get_insert_dicts_query(table_name, dicts) -%}
    {% set insert_dicts_query %}
        insert into {{ table_name }}
            {% set columns = adapter.get_columns_in_relation(table_name) -%}
            ({%- for column in columns -%}
                {{- column.name -}} {{- "," if not loop.last else "" -}}
            {%- endfor -%}) values
            {% for dict in dicts -%}
                ({%- for column in columns -%}
                    {%- set column_value = elementary.insensitive_get_dict_value(dict, column.name, none) -%}
                    {{ elementary.render_value(column_value) }}
                    {{- "," if not loop.last else "" -}}
                 {%- endfor -%}) {{- "," if not loop.last else "" -}}
            {%- endfor -%}
    {% endset %}
    {{ return(insert_dicts_query) }}
{%- endmacro %}

{%- macro escape_special_chars(string_value) -%}
    {{- return(string_value | replace("\\", "\\\\") | replace("'", "\\'") | replace("\n", "\\n")) -}}
{%- endmacro -%}

{%- macro render_value(value) -%}
    {%- if value is number -%}
        {{- value -}}
    {%- elif value is string -%}
        '{{- elementary.escape_special_chars(value) -}}'
    {%- elif value is mapping or value is sequence -%}
        '{{- elementary.escape_special_chars(tojson(value)) -}}'
    {%- else -%}
        NULL
    {%- endif -%}
{%- endmacro -%}

