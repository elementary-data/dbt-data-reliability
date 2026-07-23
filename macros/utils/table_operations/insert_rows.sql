{% macro insert_rows(
    table_relation,
    rows,
    should_commit=false,
    chunk_size=5000,
    on_query_exceed=none
) %}
    {{
        return(
            adapter.dispatch("insert_rows", "elementary")(
                table_relation, rows, should_commit, chunk_size, on_query_exceed
            )
        )
    }}
{% endmacro %}

{% macro default__insert_rows(
    table_relation,
    rows,
    should_commit=false,
    chunk_size=5000,
    on_query_exceed=none
) %}
    {% do elementary.begin_duration_measure_context("insert_rows") %}

    {% if not rows %}
        {% do elementary.end_duration_measure_context("insert_rows") %}
        {{ return(none) }}
    {% endif %}

    {% if not table_relation %}
        {% do elementary.warn_missing_elementary_models() %}
        {% do elementary.end_duration_measure_context("insert_rows") %}
        {{ return(none) }}
    {% endif %}

    {% do elementary.begin_duration_measure_context("get_columns_in_relation") %}
    {% set columns = adapter.get_columns_in_relation(table_relation) %}
    {% do elementary.end_duration_measure_context("get_columns_in_relation") %}
    {% if not columns %}
        {% set table_name = elementary.relation_to_full_name(table_relation) %}
        {{
            elementary.edr_log(
                "Could not extract columns for table - "
                ~ table_name
                ~ " (might be a permissions issue)"
            )
        }}
        {% do elementary.end_duration_measure_context("insert_rows") %}
        {{ return(none) }}
    {% endif %}

    {{
        elementary.file_log(
            "Inserting {} rows to table {}".format(rows | length, table_relation)
        )
    }}

    {% set insert_rows_queries = elementary.get_insert_rows_queries(
        table_relation,
        columns,
        rows,
        chunk_size=chunk_size,
        on_query_exceed=on_query_exceed,
    ) %}
    {% set queries_len = insert_rows_queries | length %}
    {% for insert_query in insert_rows_queries %}
        {% do elementary.file_log(
            "[{}/{}] Running insert query.".format(loop.index, queries_len)
        ) %}
        {% do elementary.begin_duration_measure_context("run_insert_rows_query") %}
        {% do elementary.run_query(insert_query) %}
        {% do elementary.end_duration_measure_context("run_insert_rows_query") %}
    {% endfor %}

    {% if should_commit %}
        {% do elementary.begin_duration_measure_context("commit") %}
        {% do adapter.commit() %}
        {% do elementary.end_duration_measure_context("commit") %}
    {% endif %}

    {% do elementary.end_duration_measure_context("insert_rows") %}
{% endmacro %}

{% macro trino__insert_rows(
    table_relation,
    rows,
    should_commit=false,
    chunk_size=5000,
    on_query_exceed=none
) %}
    {{
        return(
            elementary.default__insert_rows(
                table_relation, rows, false, chunk_size, on_query_exceed
            )
        )
    }}
{% endmacro %}

{% macro get_insert_rows_queries(
    table_relation,
    columns,
    rows,
    query_max_size=none,
    chunk_size=5000,
    on_query_exceed=none
) -%}
    {% do elementary.begin_duration_measure_context("get_insert_rows_queries") %}

    {% if not query_max_size %}
        {% set query_max_size = elementary.get_config_var("query_max_size") %}
    {% endif %}

    {% set insert_queries = [] %}
    {% do elementary.begin_duration_measure_context("base_query_calc") %}
    {% set base_insert_query %}
       insert into {{ table_relation }}
         ({%- for column in columns -%}
           {{- elementary.escape_reserved_keywords(column.name) -}} {{- "," if not loop.last else "" -}}
         {%- endfor -%})
         {{ elementary.get_query_settings() }}
         values
    {% endset %}
    {% do elementary.end_duration_measure_context("base_query_calc") %}

    {# Precompute per-column rendering metadata and adapter implementations once,
       hoisted out of the per-row loop (dtypes / created_at handling / dispatch
       results are identical for every row). #}
    {% set column_meta = elementary.get_row_render_metadata(
        columns, rows[0] if rows else none
    ) %}
    {% set created_at_sql = elementary.edr_current_timestamp() %}
    {% set render_value_impl = adapter.dispatch("render_value", "elementary") %}
    {% set escaper = adapter.dispatch("escape_special_chars", "elementary") %}

    {# Accumulate each chunk's rendered rows in a list and join once when the
       chunk is finalized, instead of repeatedly concatenating a growing query
       string (which is O(n^2)). A namespace is required so reassignments made
       inside the loop persist across iterations. #}
    {% set base_len = base_insert_query | length %}
    {% set chunk = namespace(rows=[], len=base_len, count=0) %}
    {% for row in rows %}
        {% set row_sql = elementary.render_row_to_sql(
            row, column_meta, created_at_sql, render_value_impl, escaper
        ) %}
        {% set row_len = row_sql | length %}
        {% set separator_len = 0 if chunk.count == 0 else 1 %}
        {% set projected_len = chunk.len + separator_len + row_len %}

        {% if projected_len > query_max_size or chunk.count >= chunk_size %}
            {% set new_query_len = base_len + row_len %}

            {# Check if row is too large to fit into an insert query. #}
            {% if new_query_len > query_max_size %}
                {% if on_query_exceed %}
                    {% do elementary.begin_duration_measure_context(
                        "on_query_exceed"
                    ) %}
                    {% do on_query_exceed(row) %}
                    {% do elementary.end_duration_measure_context("on_query_exceed") %}

                    {% set row_sql = elementary.render_row_to_sql(
                        row,
                        column_meta,
                        created_at_sql,
                        render_value_impl,
                        escaper,
                    ) %}
                    {% set row_len = row_sql | length %}
                    {% set new_query_len = base_len + row_len %}
                {% endif %}

                {% if new_query_len > query_max_size %}
                    {% do elementary.file_log(
                        "Oversized row for insert_rows: {}".format(
                            base_insert_query + row_sql
                        )
                    ) %}
                    {% do exceptions.warn(
                        "Row to be inserted exceeds var('query_max_size'). Consider increasing its value."
                    ) %}
                {% endif %}
            {% endif %}

            {% if chunk.count > 0 %}
                {% do insert_queries.append(
                    base_insert_query + (chunk.rows | join(","))
                ) %}
            {% endif %}
            {% set chunk.rows = [row_sql] %}
            {% set chunk.len = new_query_len %}
            {% set chunk.count = 1 %}

        {% else %}
            {% do chunk.rows.append(row_sql) %}
            {% set chunk.len = projected_len %}
            {% set chunk.count = chunk.count + 1 %}
        {% endif %}
        {% if loop.last %}
            {% do insert_queries.append(
                base_insert_query + (chunk.rows | join(","))
            ) %}
        {% endif %}
    {% endfor %}

    {% do elementary.end_duration_measure_context("get_insert_rows_queries") %}
    {{ return(insert_queries) }}
{%- endmacro %}

{# Precompute per-column rendering metadata once per insert so the hot per-row
   loop only does a dict lookup + value render. `sample_row` (the first row) is
   used to resolve the actual dict key for each column, replacing the per-cell
   case-insensitive lookup - flatten macros emit consistent keys across rows. #}
{% macro get_row_render_metadata(columns, sample_row) %}
    {% set column_meta = [] %}
    {% for column in columns %}
        {% set is_created_at = column.name | lower == "created_at" %}
        {% set row_key = none %}
        {% if not is_created_at and sample_row is not none %}
            {% if column.name in sample_row %} {% set row_key = column.name %}
            {% elif column.name | lower in sample_row %}
                {% set row_key = column.name | lower %}
            {% elif column.name | upper in sample_row %}
                {% set row_key = column.name | upper %}
            {% endif %}
        {% endif %}
        {% do column_meta.append(
            {
                "is_created_at": is_created_at,
                "row_key": row_key,
                "normalized_type": elementary.normalize_data_type(
                    column.dtype
                ),
            }
        ) %}
    {% endfor %}
    {% do return(column_meta) %}
{% endmacro %}

{% macro render_row_to_sql(
    row, column_meta, created_at_sql, render_value_impl, escaper
) %}
    {% set rendered_column_values = [] %}
    {% for column in column_meta %}
        {% if column.is_created_at %}
            {% do rendered_column_values.append(created_at_sql) %}
        {% else %}
            {% set column_value = (
                row.get(column.row_key)
                if column.row_key is not none
                else none
            ) %}
            {% do rendered_column_values.append(
                render_value_impl(
                    column_value, column.normalized_type, escaper
                )
            ) %}
        {% endif %}
    {% endfor %}
    {% do return("({})".format(rendered_column_values | join(","))) %}
{% endmacro %}

{% macro escape_special_chars(string_value) %}
    {{ return(adapter.dispatch("escape_special_chars", "elementary")(string_value)) }}
{% endmacro %}

{%- macro default__escape_special_chars(string_value) -%}
    {{-
        return(
            string_value
            | replace("\\", "\\\\")
            | replace("'", "\\'")
            | replace("\n", "\\n")
            | replace("\r", "\\r")
        )
    -}}
{%- endmacro -%}

{%- macro redshift__escape_special_chars(string_value) -%}
    {{-
        return(
            string_value
            | replace("\\", "\\\\")
            | replace("'", "\\'")
            | replace("\n", "\\n")
            | replace("\r", "\\r")
        )
    -}}
{%- endmacro -%}

{%- macro postgres__escape_special_chars(string_value) -%}
    {{- return(string_value | replace("'", "''")) -}}
{%- endmacro -%}

{%- macro duckdb__escape_special_chars(string_value) -%}
    {{- return(string_value | replace("'", "''")) -}}
{%- endmacro -%}

{%- macro vertica__escape_special_chars(string_value) -%}
    {{- return(string_value | replace("'", "''")) -}}
{%- endmacro -%}

{%- macro athena__escape_special_chars(string_value) -%}
    {{- return(string_value | replace("'", "''")) -}}
{%- endmacro -%}

{%- macro fabric__escape_special_chars(string_value) -%}
    {{- return(string_value | replace("'", "''")) -}}
{%- endmacro -%}

{%- macro sqlserver__escape_special_chars(string_value) -%}
    {%- do return(elementary.fabric__escape_special_chars(string_value)) -%}
{%- endmacro -%}

{%- macro dremio__escape_special_chars(string_value) -%}
    {{-
        return(
            string_value
            | replace("'", "''")
            | replace("‘", "''")
            | replace("’", "''")
            | replace("​", " ")
        )
    -}}
{%- endmacro -%}

{%- macro trino__escape_special_chars(string_value) -%}
    {{- return(string_value | replace("'", "''")) -}}
{%- endmacro -%}

{# spark__escape_special_chars: Newlines and carriage returns are replaced with
   spaces (not escape sequences) because Spark SQL does not support multi-line
   string literals inside INSERT VALUES. Backslashes and single quotes use
   C-style escaping (\\, \') which is the Spark SQL convention. #}
{%- macro spark__escape_special_chars(string_value) -%}
    {{-
        return(
            string_value
            | replace("\\", "\\\\")
            | replace("'", "\\'")
            | replace("\n", " ")
            | replace("\r", " ")
        )
    -}}
{%- endmacro -%}

{# `escaper` lets callers pass a pre-resolved escape_special_chars implementation
   so the hot insert path avoids an adapter.dispatch per rendered cell. When it's
   not provided (other callers) it is resolved once here, matching prior behavior. #}
{%- macro render_value(value, data_type, escaper=none) -%}
    {{- adapter.dispatch("render_value", "elementary")(value, data_type, escaper) -}}
{%- endmacro -%}

{%- macro default__render_value(value, data_type, escaper=none) -%}
    {%- if escaper is none -%}
        {%- set escaper = adapter.dispatch("escape_special_chars", "elementary") -%}
    {%- endif -%}
    {%- if value is defined and value is not none -%}
        {%- if value is boolean -%} {{- elementary.edr_boolean_literal(value) -}}
        {%- elif value is number -%} {{- value -}}
        {%- elif value is string and data_type == "timestamp" -%}
            {{-
                elementary.edr_cast_as_timestamp(
                    elementary.edr_datetime_to_sql(value)
                )
            -}}
        {%- elif value is string -%} '{{- escaper(value) -}}'
        {%- elif value is mapping or value is sequence -%}
            '{{- escaper(tojson(value)) -}}'
        {%- else -%} null
        {%- endif -%}
    {%- else -%} null
    {%- endif -%}
{%- endmacro -%}

{%- macro fabricspark__escape_special_chars(string_value) -%}
    {{- return(elementary.spark__escape_special_chars(string_value)) -}}
{%- endmacro -%}

{# Note: Python booleans pass Jinja's "is number" test, so we check
   "is boolean" first. edr_boolean_literal renders the correct SQL literal
   per adapter (TRUE/FALSE for standard SQL, cast(1/0 as bit) for T-SQL). #}
