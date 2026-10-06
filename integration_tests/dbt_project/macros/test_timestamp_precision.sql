{% macro test_truncate_to_microseconds(value) %}
    {{ return(elementary.truncate_to_microseconds(value)) }}
{% endmacro %}

{% macro test_render_cast_as_timestamp(column_name) %}
    {{ return(elementary.edr_cast_as_timestamp(column_name)) }}
{% endmacro %}

{% macro test_cast_as_timestamp(value) %}
    {% set query %}
        select {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(value)) }} as ts
    {% endset %}
    {% set ts = elementary.run_query(query).columns[0].values()[0] %}
    {{ return(ts.isoformat()) }}
{% endmacro %}

{# Inserts a row with nanosecond timing values, then returns the timing values
   before and after running fix_nanosecond_timing_values. #}
{% macro test_fix_nanosecond_timing_values(table_name, id_column, row_id, value) %}
    {% set relation = elementary.get_elementary_relation(table_name) %}
    {% set timing_columns = [
        "execute_started_at",
        "execute_completed_at",
        "compile_started_at",
        "compile_completed_at",
    ] %}
    {% set insert_query %}
        insert into {{ relation }} ({{ id_column }}, {{ timing_columns | join(", ") }})
        values (
            {{ elementary.edr_quote(row_id) }}
            {% for column in timing_columns %}, {{ elementary.edr_quote(value) }}{% endfor %}
        )
    {% endset %}
    {% set select_query %}
        select {{ timing_columns | join(", ") }} from {{ relation }}
        where {{ id_column }} = {{ elementary.edr_quote(row_id) }}
    {% endset %}
    {% set delete_query %}
        delete from {{ relation }}
        where {{ id_column }} = {{ elementary.edr_quote(row_id) }}
    {% endset %}

    {% do elementary.run_query(insert_query) %}
    {% set before = elementary.run_query(select_query).rows[0] | list %}
    {% do elementary.fix_nanosecond_timing_values() %}
    {% set after = elementary.run_query(select_query).rows[0] | list %}
    {% do elementary.run_query(delete_query) %}
    {{ return({"before": before, "after": after}) }}
{% endmacro %}
