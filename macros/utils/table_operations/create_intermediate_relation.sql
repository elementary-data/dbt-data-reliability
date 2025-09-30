{% macro create_intermediate_relation(base_relation, rows, temporary, like_columns=none) %}
    {% set int_relation = elementary.edr_make_intermediate_relation(base_relation) %}

    {# It seems that in dbt-fusion we fail in case the database/schema are None and not passed explicitly
       through the "path" param (happens in temp tables for some adapters). 
       So to be safe, we just pass all of them explicitly. #}
    {% set int_relation = int_relation.incorporate(
        type='table',
        path={"database": int_relation.database,
              "schema": int_relation.schema,
              "table": int_relation.identifier}
    ) %}

    {% if not elementary.has_temp_table_support() %}
        {% set temporary = false %}
    {% endif %}

    {% do elementary.create_table_like(int_relation, base_relation, temporary, like_columns) %}
    {% do elementary.insert_rows(int_relation, rows, should_commit=false, chunk_size=elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    {% do return(int_relation) %}
{% endmacro %}

{% macro edr_make_intermediate_relation(base_relation) %}
    {% do return(adapter.dispatch("edr_make_intermediate_relation", "elementary")(base_relation)) %}
{% endmacro %}

{% macro default__edr_make_intermediate_relation(base_relation) %}
    {% do return(elementary.make_temp_table_relation(base_relation)) %}
{% endmacro %}

{% macro databricks__edr_make_intermediate_relation(base_relation) %}
    {% set tmp_identifier = elementary.table_name_with_suffix(base_relation.identifier, elementary.get_timestamped_table_suffix()) %}
    {% set tmp_relation = api.Relation.create(
        identifier=tmp_identifier,
        schema=base_relation.schema,
        database=base_relation.database,
        type='table') %}
    {% do return(tmp_relation) %}
{% endmacro %}
