{% macro insert_as_select(table_relation, select_query) %}
    {# when calling this macro, you need to add depends on ref comment #}
    {# ref_model and select_query need to have the same columns #}
    {%- if target.type in ["fabric", "sqlserver"] -%}
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
    {%- else -%}
        {%- set insert_query %}
            insert into {{ table_relation }}
            with tmp_table as (
                {{ select_query }}
            )
            select * from tmp_table
            {{ elementary.get_query_settings() }}
        {%- endset %}
    {%- endif -%}

    {{ return(insert_query) }}

{% endmacro %}
