{% macro edr_make_temp_relation(base_relation, suffix=none) %}
    {% do return(adapter.dispatch("edr_make_temp_relation", "elementary")(base_relation, suffix)) %}
{% endmacro %}

{% macro default__edr_make_temp_relation(base_relation, suffix) %}
    {% do return(dbt.make_temp_relation(base_relation, suffix)) %}
{% endmacro %}

{% macro spark__edr_make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(path = {
        "schema": none,
        "identifier": tmp_identifier
    }) -%}

    {% do return(tmp_relation) %}
{% endmacro %}

--- VIEWS
{% macro make_temp_view_relation(base_relation, suffix=none) %}
    {% if not suffix %}
        {% set suffix = modules.datetime.datetime.utcnow().strftime('__tmp_%Y%m%d%H%M%S%f') %}
    {% endif %}

    {% do return(elementary.edr_make_temp_relation(base_relation, suffix)) %}
{% endmacro %}


--- TABLES
{% macro make_temp_table_relation(base_relation, suffix=none) %}
    {% if not suffix %}
        {% set suffix = modules.datetime.datetime.utcnow().strftime('__tmp_%Y%m%d%H%M%S%f') %}
    {% endif %}

    {% do return(adapter.dispatch("make_temp_table_relation", "elementary")(base_relation, suffix)) %}
{% endmacro %}

{% macro default__make_temp_table_relation(base_relation, suffix) %}
    {% do return(elementary.edr_make_temp_relation(base_relation, suffix)) %}
{% endmacro %}

{% macro databricks__make_temp_table_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% set tmp_relation = api.Relation.create(
        identifier=tmp_identifier,
        schema=base_relation.schema,
        database=base_relation.database,
        type='table') %}
    {% do return(tmp_relation) %}
{% endmacro %}

