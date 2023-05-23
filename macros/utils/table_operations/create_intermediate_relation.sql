{% macro create_intermediate_relation(base_relation, rows, temporary, like_columns=none) %}
    {% set int_relation = elementary.make_temp_table_relation(base_relation).incorporate(type='table') %}

    {% if not elementary.has_temp_table_support() %}
        {% set temporary = false %}
    {% endif %}

    {% do elementary.create_table_like(int_relation, base_relation, temporary, like_columns) %}
    {% do elementary.insert_rows(int_relation, rows, should_commit=false, chunk_size=elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    {% do return(int_relation) %}
{% endmacro %}
