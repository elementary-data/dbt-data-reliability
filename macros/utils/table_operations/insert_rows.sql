{% macro insert_rows(table_relation, rows, should_commit=false, chunk_size=5000, on_query_exceed=none) %}
    {{ return(adapter.dispatch('insert_rows', 'elementary')(table_relation, rows, should_commit, chunk_size, on_query_exceed)) }}
{% endmacro %}

{% macro default__insert_rows(table_relation, rows, should_commit=false, chunk_size=5000, on_query_exceed=none) %}
    {% do elementary.begin_duration_measure_context('insert_rows') %}

    {% if not rows %}
      {% do elementary.end_duration_measure_context('insert_rows') %}
      {{ return(none) }}
    {% endif %}

    {% if not table_relation %}
        {% do elementary.warn_missing_elementary_models() %}
        {% do elementary.end_duration_measure_context('insert_rows') %}
        {{ return(none) }}
    {% endif %}

    {% do elementary.begin_duration_measure_context('get_columns_in_relation') %}
    {% set columns = adapter.get_columns_in_relation(table_relation) %}
    {% do elementary.end_duration_measure_context('get_columns_in_relation') %}
    {% if not columns %}
        {% set table_name = elementary.relation_to_full_name(table_relation) %}
        {{ elementary.edr_log('Could not extract columns for table - ' ~ table_name ~ ' (might be a permissions issue)') }}
        {% do elementary.end_duration_measure_context('insert_rows') %}
        {{ return(none) }}
    {% endif %}

    {{ elementary.file_log('Inserting {} rows to table {}'.format(rows | length, table_relation)) }}
    {% set insert_rows_method = elementary.get_config_var('insert_rows_method') %}
    {% if insert_rows_method == 'max_query_size' %}
      {% set insert_rows_queries = elementary.get_insert_rows_queries(table_relation, columns, rows, on_query_exceed=on_query_exceed) %}
      {% set queries_len = insert_rows_queries | length %}
      {% for insert_query in insert_rows_queries %}
        {% do elementary.file_log("[{}/{}] Running insert query.".format(loop.index, queries_len)) %}
        {% do elementary.begin_duration_measure_context('run_insert_rows_query') %}
        {% do elementary.run_query(insert_query) %}
        {% do elementary.end_duration_measure_context('run_insert_rows_query') %}
      {% endfor %}
    {% elif insert_rows_method == 'chunk' %}
      {% set rows_chunks = elementary.split_list_to_chunks(rows, chunk_size) %}
      {% for rows_chunk in rows_chunks %}
        {% do elementary.begin_duration_measure_context('get_chunk_insert_query') %}
        {% set insert_rows_query = elementary.get_chunk_insert_query(table_relation, columns, rows_chunk) %}
        {% do elementary.end_duration_measure_context('get_chunk_insert_query') %}

        {% do elementary.begin_duration_measure_context('run_chunk_insert_query') %}
        {% do elementary.run_query(insert_rows_query) %}
        {% do elementary.end_duration_measure_context('run_chunk_insert_query') %}
      {% endfor %}
    {% else %}
      {% do exceptions.raise_compiler_error("Specified invalid value for 'insert_rows_method' var.") %}
    {% endif %}

    {% if should_commit %}
      {% do elementary.begin_duration_measure_context('commit') %}
      {% do adapter.commit() %}
      {% do elementary.end_duration_measure_context('commit') %}
    {% endif %}

    {% do elementary.end_duration_measure_context('insert_rows') %}
{% endmacro %}

{% macro trino__insert_rows(table_relation, rows, should_commit=false, chunk_size=5000, on_query_exceed=none) %}
    {{ return(elementary.default__insert_rows(table_relation, rows, false, chunk_size, on_query_exceed)) }}
{% endmacro %}

{% macro get_insert_rows_queries(table_relation, columns, rows, query_max_size=none, on_query_exceed=none) -%}
    {% do elementary.begin_duration_measure_context('get_insert_rows_queries') %}

    {% if not query_max_size %}
      {% set query_max_size = elementary.get_config_var('query_max_size') %}
    {% endif %}

    {% set insert_queries = [] %}
    {% do elementary.begin_duration_measure_context('base_query_calc') %}
    {% set base_insert_query %}
       insert into {{ table_relation }}
         ({%- for column in columns -%}
           {{- column.name -}} {{- "," if not loop.last else "" -}}
         {%- endfor -%}) values
    {% endset %}
    {% do elementary.end_duration_measure_context('base_query_calc') %}

    {% set current_query = namespace(data=base_insert_query) %}
    {% for row in rows %}
      {% set row_sql = elementary.render_row_to_sql(row, columns) %}
      {% set query_with_row = current_query.data + ("," if not loop.first else "") + row_sql %}

      {% if query_with_row | length > query_max_size %}
        {% set new_insert_query = base_insert_query + row_sql %}

        {# Check if row is too large to fit into an insert query. #}
        {% if new_insert_query | length > query_max_size %}
          {% if on_query_exceed %}
            {% do elementary.begin_duration_measure_context('on_query_exceed') %}
            {% do on_query_exceed(row) %}
            {% do elementary.end_duration_measure_context('on_query_exceed') %}

            {% set row_sql = elementary.render_row_to_sql(row, columns) %}
            {% set new_insert_query = base_insert_query + row_sql %}
          {% endif %}

          {% if new_insert_query | length > query_max_size %}
            {% do elementary.file_log("Oversized row for insert_rows: {}".format(query_with_row)) %}
            {% do exceptions.warn("Row to be inserted exceeds var('query_max_size'). Consider increasing its value.") %}
          {% endif %}
        {% endif %}

        {% if current_query.data != base_insert_query %}
          {% do insert_queries.append(current_query.data) %}
        {% endif %}
        {% set current_query.data = new_insert_query %}

      {% else %}
        {% set current_query.data = query_with_row %}
      {% endif %}
      {% if loop.last %}
        {% do insert_queries.append(current_query.data) %}
      {% endif %}
    {% endfor %}

    {% do elementary.end_duration_measure_context('get_insert_rows_queries') %}
    {{ return(insert_queries) }}
{%- endmacro %}

{% macro render_row_to_sql(row, columns) %}
  {% do elementary.begin_duration_measure_context('render_row_to_sql') %}

  {% set rendered_column_values = [] %}
  {% for column in columns %}
    {% if column.name.lower() == "created_at" %}
      {% set column_value = elementary.edr_current_timestamp() %}
      {% do rendered_column_values.append(column_value) %}
    {% else %}
      {% set column_value = elementary.insensitive_get_dict_value(row, column.name) %}
      {% set normalized_data_type = elementary.normalize_data_type(column.dtype) %}
      {% do rendered_column_values.append(elementary.render_value(column_value, normalized_data_type)) %}

    {% endif %}
  {% endfor %}
  {% set row_sql = "({})".format(rendered_column_values | join(",")) %}

  {% do elementary.end_duration_measure_context('render_row_to_sql') %}
  {% do return(row_sql) %}
{% endmacro %}

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

{% macro escape_special_chars(string_value) %}
    {{ return(adapter.dispatch('escape_special_chars', 'elementary')(string_value)) }}
{% endmacro %}

{%- macro default__escape_special_chars(string_value) -%}
    {{- return(string_value | replace("\\", "\\\\") | replace("'", "\\'") | replace("\n", "\\n") | replace("\r", "\\r")) -}}
{%- endmacro -%}

{%- macro redshift__escape_special_chars(string_value) -%}
    {{- return(string_value | replace("\\", "\\\\") | replace("'", "\\'") | replace("\n", "\\n") | replace("\r", "\\r")) -}}
{%- endmacro -%}

{%- macro postgres__escape_special_chars(string_value) -%}
    {{- return(string_value | replace("'", "''")) -}}
{%- endmacro -%}

{%- macro athena__escape_special_chars(string_value) -%}
    {{- return(string_value | replace("'", "''")) -}}
{%- endmacro -%}

{%- macro trino__escape_special_chars(string_value) -%}
    {{- return(string_value | replace("'", "''")) -}}
{%- endmacro -%}

{%- macro render_value(value, data_type) -%}
    {%- if value is defined and value is not none -%}
        {%- if value is number -%}
            {{- value -}}
        {%- elif value is string and data_type == 'timestamp' -%}
            {{- elementary.edr_cast_as_timestamp(elementary.edr_quote(value)) -}}
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
