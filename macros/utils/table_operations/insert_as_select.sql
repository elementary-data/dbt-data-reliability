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
    {#- Fabric does not support INSERT ... EXEC or CTEs after INSERT INTO.
        Wrap the select_query in a temp view, then INSERT ... SELECT from it.
        Fabric also forbids 3-part names in DROP VIEW, so use schema.identifier only.

        NOTE: The replace("'", "''") escaping is minimal — if select_query already
        contains escaped quotes (e.g. from user-defined test configs), this could
        double-escape and produce invalid SQL. In practice the queries passed here
        are machine-generated and do not contain pre-escaped quotes. -#}
    {%- set tmp_view_name = (
        table_relation.schema ~ "." ~ table_relation.identifier ~ "__ins_vw"
    ) -%}
    {%- set insert_query %}
        IF OBJECT_ID('{{ tmp_view_name }}', 'V') IS NOT NULL DROP VIEW {{ tmp_view_name }};
        EXEC('CREATE VIEW {{ tmp_view_name }} AS {{ select_query | replace("'", "''") }}');
        INSERT INTO {{ table_relation }}
        SELECT * FROM {{ tmp_view_name }};
        DROP VIEW {{ tmp_view_name }};
    {%- endset %}
    {{ return(insert_query) }}
{% endmacro %}
