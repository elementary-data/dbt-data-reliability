{% macro insert_as_select(ref_model, select_query) %}
    {# When calling this macro, you need to add depends on ref comment #}
    {# ref_model and select_query need to have the same columns #}

    {%- if table_exists_in_target(ref_model) %}
        {%- set insert_query %}
            insert into {{ ref(ref_model) }}
            with tmp_table as (
                {{ select_query }}
            )
            select * from tmp_table
        {%- endset %}
    {%- else %}
        {%- set insert_query = null %}
    {%- endif %}

    {{ return(insert_query) }}

{% endmacro %}