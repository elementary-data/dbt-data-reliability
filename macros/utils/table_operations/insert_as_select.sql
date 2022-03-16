{% macro insert_as_select(full_table_name, select_query) %}
    {# when calling this macro, you need to add depends on ref comment #}
    {# ref_model and select_query need to have the same columns #}

    {%- set insert_query %}
        insert into {{ elementary.from(full_table_name) }}
        with tmp_table as (
            {{ select_query }}
        )
        select * from tmp_table
    {%- endset %}

    {{ return(insert_query) }}

{% endmacro %}