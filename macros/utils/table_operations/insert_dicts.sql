{% macro insert_dicts(table_relation, dicts, chunk_size=50, should_commit=False) %}
    {% if not table_relation %}
        {{ elementary.edr_log('Recieved table relation is not valid (make sure elementary models were executed successfully first)') }}
        {{ return(none) }}
    {% endif %}
    {% set columns = adapter.get_columns_in_relation(table_relation) -%}
    {% if not columns %}
        {% set table_name = elementary.relation_to_full_name(table_relation) %}
        {{ elementary.edr_log('Could not extract columns for table - ' ~ table_name ~ ' (might be a permissions issue)') }}
        {{ return(none) }}
    {% endif %}
    {% set dicts_chunks = elementary.split_list_to_chunks(dicts, chunk_size) %}
    {% for dicts_chunk in dicts_chunks %}
        {% set insert_dicts_query = elementary.get_insert_dicts_query(table_relation, columns, dicts_chunk) %}
        {% do run_query(insert_dicts_query) %}
    {% endfor %}
    {%- if should_commit -%}
        {% do adapter.commit() %}
    {%- endif -%}
{% endmacro %}

{% macro get_insert_dicts_query(table_relation, columns, dicts) -%}
    {% set insert_dicts_query %}
        insert into {{ table_relation }}
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
    {{- return(string_value | replace("\\", "\\\\") | replace("'", "\\'") | replace("\n", "\\n") | replace("\r", "\\r")) -}}
{%- endmacro -%}

{%- macro render_value(value) -%}
    {%- if value is defined and value is not none -%}
        {%- if value is number -%}
            {{- value -}}
        {%- elif value is string -%}
            '{{- elementary.escape_special_chars(value) -}}'
        {%- elif value is mapping or value is sequence -%}
            '{{- elementary.escape_special_chars(tojson(value)) -}}'
        {%- else -%}
            NULL
        {%- endif -%}
    {%- else -%}
        NULL
    {%- endif -%}
{%- endmacro -%}

