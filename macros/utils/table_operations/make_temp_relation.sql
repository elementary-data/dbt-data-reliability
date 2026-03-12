{% macro edr_make_temp_relation(base_relation, suffix=none) %}
    {% do return(
        adapter.dispatch("edr_make_temp_relation", "elementary")(
            base_relation, suffix
        )
    ) %}
{% endmacro %}

{% macro default__edr_make_temp_relation(base_relation, suffix) %}
    {% do return(dbt.make_temp_relation(base_relation, suffix)) %}
{% endmacro %}

{% macro fabric__edr_make_temp_relation(base_relation, suffix) %}
    {#
        In some contexts (notably test materializations), callers may pass the dbt node
        (a dict) rather than a Relation. dbt.make_temp_relation expects a Relation and
        will fail with "dict object has no attribute incorporate".

        For Fabric / T-SQL, we can safely create a regular table relation in the active
        target schema and treat it as our "temp" relation.
    #}
    {% if not suffix %}
        {% set suffix = elementary.get_timestamped_table_suffix() %}
    {% endif %}

    {% if base_relation is mapping %}
        {# Prefer the Elementary package schema, which we know exists in the project. #}
        {% set package_database, package_schema = (
            elementary.get_package_database_and_schema()
        ) %}

        {% set base_identifier = (
            base_relation.get("alias")
            or base_relation.get("name")
            or "edr_tmp"
        ) %}
        {% set tmp_identifier = elementary.table_name_with_suffix(
            base_identifier, suffix
        ) %}
        {% set tmp_relation = api.Relation.create(
            database=package_database
            or base_relation.get("database")
            or target.database,
            schema=package_schema
            or base_relation.get("schema")
            or target.schema,
            identifier=tmp_identifier,
            type="table",
        ) %}
        {% do return(tmp_relation) %}
    {% endif %}

    {% do return(dbt.make_temp_relation(base_relation, suffix)) %}
{% endmacro %}

{% macro spark__edr_make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = elementary.table_name_with_suffix(
        base_relation.identifier, suffix
    ) %}
    {% set tmp_relation = base_relation.incorporate(
        path={"schema": none, "identifier": tmp_identifier}
    ) -%}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro fabricspark__edr_make_temp_relation(base_relation, suffix) %}
    {{ return(elementary.spark__edr_make_temp_relation(base_relation, suffix)) }}
{% endmacro %}

{% macro redshift__edr_make_temp_relation(base_relation, suffix) %}
    {% if elementary.is_dbt_fusion() %}
        {# Workaround for dbt-fusion temp table metadata bug - create regular relations
           with explicit schema/database instead of temp relations #}
        {% set tmp_identifier = elementary.table_name_with_suffix(
            base_relation.identifier, suffix
        ) %}
        {% set tmp_relation = api.Relation.create(
            identifier=tmp_identifier,
            schema=base_relation.schema,
            database=base_relation.database,
            type="table",
        ) %}
        {% do return(tmp_relation) %}
    {% else %} {% do return(dbt.make_temp_relation(base_relation, suffix)) %}
    {% endif %}
{% endmacro %}

{% macro databricks__edr_make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = elementary.table_name_with_suffix(
        base_relation.identifier, suffix
    ) %}
    {% if elementary.is_dbt_fusion() %}
        {# In dbt-fusion, the view will be created as non-temporary. Therefore, we need the relation to include database and schema.
           So we use the same ones as the original relation and just change the identifier. #}
        {% set tmp_relation = base_relation.incorporate(
            path={"identifier": tmp_identifier}, type="view"
        ) %}
    {% else %}
        {% set tmp_relation = api.Relation.create(
            identifier=tmp_identifier, type="view"
        ) %}
    {% endif %}
    {% do return(tmp_relation) %}
{% endmacro %}

{% macro dremio__edr_make_temp_relation(base_relation, suffix) %}
    {% set base_relation_with_type = base_relation.incorporate(type="table") %}
    {% do return(dbt.make_temp_relation(base_relation_with_type, suffix)) %}
{% endmacro %}

-- - VIEWS
{% macro make_temp_view_relation(base_relation, suffix=none) %}
    {% if not suffix %}
        {% set suffix = elementary.get_timestamped_table_suffix() %}
    {% endif %}

    {% do return(elementary.edr_make_temp_relation(base_relation, suffix)) %}
{% endmacro %}


-- - TABLES
{% macro make_temp_table_relation(base_relation, suffix=none) %}
    {% if not suffix %}
        {% set suffix = elementary.get_timestamped_table_suffix() %}
    {% endif %}

    {% do return(
        adapter.dispatch("make_temp_table_relation", "elementary")(
            base_relation, suffix
        )
    ) %}
{% endmacro %}

{% macro default__make_temp_table_relation(base_relation, suffix) %}
    {% do return(elementary.edr_make_temp_relation(base_relation, suffix)) %}
{% endmacro %}

{% macro clickhouse__make_temp_table_relation(base_relation, suffix) %}
    {% set tmp_identifier = elementary.table_name_with_suffix(
        base_relation.identifier, suffix
    ) %}
    {% set tmp_relation = api.Relation.create(
        identifier=tmp_identifier,
        schema=base_relation.schema,
        database=base_relation.database,
        type="table",
        can_on_cluster=base_relation.can_on_cluster,
        can_exchange=base_relation.can_exchange,
    ) %}
    {% do return(tmp_relation) %}
{% endmacro %}
