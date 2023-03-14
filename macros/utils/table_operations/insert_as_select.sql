{% macro insert_as_select(table_relation, select_query) -%}
    {{ return(adapter.dispatch('insert_as_select', 'elementary')(table_relation, select_query)) }}
{%- endmacro %}

{% macro default__insert_as_select(table_relation, select_query) %}
    {# when calling this macro, you need to add depends on ref comment #}
    {# ref_model and select_query need to have the same columns #}

    {%- set insert_query %}
        insert into {{ table_relation }}
        with tmp_table as (
            {{ select_query }}
        )
        select * from tmp_table
    {%- endset %}

    {{ return(insert_query) }}

{% endmacro %}

{% macro sqlserver__insert_as_select(table_relation, select_query) %}
    {# when calling this macro, you need to add depends on ref comment #}
    {# ref_model and select_query need to have the same columns #}

    {%- set insert_query %}
        insert into {{ table_relation }}
        select * from ({{ select_query }}) tmp_table
    {%- endset %}

    {{ return(insert_query) }}

{% endmacro %}