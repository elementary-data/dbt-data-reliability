{% macro edr_make_temp_relation(base_relation, suffix=none) %}
    {% do return(adapter.dispatch("edr_make_temp_relation", "elementary")(base_relation, suffix)) %}
{% endmacro %}

{% macro default__edr_make_temp_relation(base_relation, suffix) %}
    {% do return(dbt.make_temp_relation(base_relation, suffix)) %}
{% endmacro %}

{% macro spark__edr_make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = elementary.table_name_with_suffix(base_relation.identifier, suffix) %}
    {% set tmp_relation = base_relation.incorporate(path = {
        "schema": none,
        "identifier": tmp_identifier
    }) -%}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro databricks__edr_make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = elementary.table_name_with_suffix(base_relation.identifier, suffix) %}
    {% if elementary.is_dbt_fusion() %}
        {# In dbt-fusion, the view will be created as non-temporary. Therefore, we need the relation to include database and schema.
           So we use the same ones as the original relation and just change the identifier. #}
        {% set tmp_relation = base_relation.incorporate(path={"identifier": tmp_identifier}, type='view') %}
    {% else %}    
        {% set tmp_relation = api.Relation.create(identifier=tmp_identifier, type='view') %}
    {% endif %}
    {% do return(tmp_relation) %}
{% endmacro %}

{% macro dremio__edr_make_temp_relation(base_relation, suffix) %}
    {% set base_relation_with_type = base_relation.incorporate(type='table') %}
    {% do return(dbt.make_temp_relation(base_relation_with_type, suffix)) %}
{% endmacro %}

--- VIEWS
{% macro make_temp_view_relation(base_relation, suffix=none) %}
    {% if not suffix %}
        {% set suffix = elementary.get_timestamped_table_suffix() %}
    {% endif %}

    {% do return(elementary.edr_make_temp_relation(base_relation, suffix)) %}
{% endmacro %}


--- TABLES
{% macro make_temp_table_relation(base_relation, suffix=none) %}
    {% if not suffix %}
        {% set suffix = elementary.get_timestamped_table_suffix() %}
    {% endif %}

    {% do return(adapter.dispatch("make_temp_table_relation", "elementary")(base_relation, suffix)) %}
{% endmacro %}

{% macro default__make_temp_table_relation(base_relation, suffix) %}
    {% do return(elementary.edr_make_temp_relation(base_relation, suffix)) %}
{% endmacro %}

{% macro clickhouse__make_temp_table_relation(base_relation, suffix) %}
    {% set tmp_identifier = elementary.table_name_with_suffix(base_relation.identifier, suffix) %}
    {% set tmp_relation = api.Relation.create(
        identifier=tmp_identifier,
        schema=base_relation.schema,
        database=base_relation.database,
        type='table') %}
    {% do return(tmp_relation) %}
{% endmacro %}
