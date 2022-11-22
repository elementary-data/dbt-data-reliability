{% macro insert_rows(table_relation, rows, should_commit=false, chunk_size=5000) %}
    {% if not rows %}
      {{ return(none) }}
    {% endif %}

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

    {{ elementary.debug_log('Inserting {} rows to table {}'.format(rows | length, table_relation)) }}
    {% set insert_rows_method = elementary.get_config_var('insert_rows_method') %}
    {% if insert_rows_method == 'max_query_size' %}
      {% set insert_rows_queries = elementary.get_insert_rows_queries(table_relation, columns, rows) %}
      {% set queries_len = insert_rows_queries | length %}
      {% for insert_query in insert_rows_queries %}
        {% do elementary.debug_log("[%d/%d] Running insert query." % (loop.index, queries_len)) %}
        {% do dbt.run_query(insert_query) %}
      {% endfor %}
    {% elif insert_rows_method == 'chunk' %}
      {% set rows_chunks = elementary.split_list_to_chunks(rows, chunk_size) %}
      {% for rows_chunk in rows_chunks %}
        {% set insert_rows_query = elementary.get_chunk_insert_query(table_relation, columns, rows_chunk) %}
        {% do run_query(insert_rows_query) %}
      {% endfor %}
    {% else %}
      {% do exceptions.raise_compiler_error("Specified invalid value for 'insert_rows_method' var.") %}
    {% endif %}

    {% if should_commit %}
      {% do adapter.commit() %}
    {% endif %}
{% endmacro %}

{% macro get_insert_rows_queries(table_relation, columns, rows, query_max_size=elementary.get_config_var('query_max_size')) -%}
    {% set insert_queries = [] %}
    {% set insert_query %}
       insert into {{ table_relation }}
         ({%- for column in columns -%}
           {{- column.name -}} {{- "," if not loop.last else "" -}}
         {%- endfor -%}) values
    {% endset %}

    {% set current_query = namespace(data=insert_query) %}
    {% for row in rows %}
      {% set rendered_column_values = [] %}
      {% for column in columns %}
        {% set column_value = elementary.insensitive_get_dict_value(row, column.name) %}
        {% do rendered_column_values.append(elementary.render_value(column_value)) %}
      {% endfor %}
      {% set row_sql = "(%s)" % (rendered_column_values | join(",")) %}
      {% set query_with_row = current_query.data + ("," if not loop.first else "") + row_sql %}

      {% if query_with_row | length > query_max_size %}
        {% if loop.first %}
          {% do exceptions.raise_compiler_error("First row to be inserted exceeds 'query_max_size'. Consider increasing its value.") %}
        {% endif %}
        {% do insert_queries.append(current_query.data) %}
        {% set current_query.data = insert_query + row_sql %}
      {% else %}
        {% set current_query.data = query_with_row %}
      {% endif %}
      {% if loop.last %}
        {% do insert_queries.append(current_query.data) %}
      {% endif %}
    {% endfor %}

    {{ return(insert_queries) }}
{%- endmacro %}

{% macro get_chunk_insert_query(table_relation, columns, rows) -%}
    {% set insert_rows_query %}
        insert into {{ table_relation }}
            ({%- for column in columns -%}
                {{- column.name -}} {{- "," if not loop.last else "" -}}
            {%- endfor -%}) values
            {% for row in rows -%}
                ({%- for column in columns -%}
                    {%- set column_value = elementary.insensitive_get_dict_value(row, column.name, none) -%}
                    {{ elementary.render_value(column_value) }}
                    {{- "," if not loop.last else "" -}}
                 {%- endfor -%}) {{- "," if not loop.last else "" -}}
            {%- endfor -%}
    {% endset %}
    {{ return(insert_rows_query) }}
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
