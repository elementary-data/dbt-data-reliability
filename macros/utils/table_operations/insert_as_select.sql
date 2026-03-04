{% macro insert_as_select(table_relation, select_query) %}
    {# when calling this macro, you need to add depends on ref comment #}
    {# ref_model and select_query need to have the same columns #}
    {{
        return(
            adapter.dispatch("insert_as_select", "elementary")(
                table_relation, select_query
            )
        )
    }}
{% endmacro %}

{% macro default__insert_as_select(table_relation, select_query) %}
    {%- set insert_query %}
        insert into {{ table_relation }}
        with tmp_table as (
            {{ select_query }}
        )
        select * from tmp_table
        {{ elementary.get_query_settings() }}
    {%- endset %}
    {{ return(insert_query) }}
{% endmacro %}

{% macro fabric__insert_as_select(table_relation, select_query) %}
    {#- T-SQL does not allow CTEs after INSERT INTO (WITH is parsed as a
        table hint), and nested CTEs are also invalid.
        Use INSERT ... EXEC(sp_executesql ...) to execute the select_query
        as a nested batch whose result set feeds the INSERT. -#}
    {%- set escaped_query = select_query | replace("'", "''") -%}
    {%- set insert_query %}
        insert into {{ table_relation }}
        exec sp_executesql N'{{ escaped_query }}'
        {{ elementary.get_query_settings() }}
    {%- endset %}
    {{ return(insert_query) }}
{% endmacro %}
