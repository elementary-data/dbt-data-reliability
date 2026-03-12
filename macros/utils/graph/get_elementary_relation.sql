{% macro get_elementary_relation(identifier) %}
    {%- if execute %}
        {%- set identifier_node = elementary.get_node(
            "model.elementary." ~ identifier
        ) %}
        {%- if identifier_node -%}
            {%- set identifier_alias = elementary.safe_get_with_default(
                identifier_node, "alias", identifier
            ) %}
            {% set elementary_database, elementary_schema = (
                identifier_node.database,
                identifier_node.schema,
            ) %}
        {%- else -%}
            {% set identifier_alias = identifier %}
            {% set elementary_database, elementary_schema = (
                elementary.get_package_database_and_schema()
            ) %}
        {%- endif -%}
        {% if this and this.database == elementary_database and this.schema == elementary_schema and this.identifier == identifier_alias %}
            {% do return(this) %}
        {% endif %}
        {% set rel = adapter.get_relation(
            elementary_database, elementary_schema, identifier_alias
        ) %}
        {# Defensive fallback: some adapters (e.g. dbt-fabricspark) have bugs in
           list_relations_without_caching that cause adapter.get_relation() to
           return None even when the relation exists.  Fall back to
           api.Relation.create() so downstream code still gets a usable ref. #}
        {% if rel is none %}
            {% set rel = api.Relation.create(
                database=elementary_database,
                schema=elementary_schema,
                identifier=identifier_alias,
            ) %}
        {% endif %}
        {% do return(rel) %}
    {%- endif %}
{% endmacro %}
