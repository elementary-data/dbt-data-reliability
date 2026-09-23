{% macro test_temp_table_expiration() %}
    {% set relation = api.Relation.create(
        database=target.database,
        schema=target.schema,
        identifier="edr_expiration_render_only",
        type="table",
    ) %}

    {# Rendering only - none of these statements are executed. #}
    {% set cases = {
        "temp_default": elementary.edr_get_create_table_as_sql(
            true, relation, "select 1 as id"
        ),
        "temp_explicit": elementary.edr_get_create_table_as_sql(
            true, relation, "select 1 as id", expiration_hours=6
        ),
        "non_temp_default": elementary.edr_get_create_table_as_sql(
            false, relation, "select 1 as id"
        ),
        "non_temp_explicit": elementary.edr_get_create_table_as_sql(
            false, relation, "select 1 as id", expiration_hours=6
        ),
    } %}

    {% do return(cases) %}
{% endmacro %}
