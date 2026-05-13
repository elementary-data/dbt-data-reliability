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
        {% set relation = adapter.get_relation(
            elementary_database, elementary_schema, identifier_alias
        ) %}
        {% if relation is not none %} {% do return(relation) %} {% endif %}
        {# Relation not found in the target schema. This can happen when
           dbt deferral (--favor-state / --defer) is active and the
           Elementary models exist in the deferred (e.g. prod) schema
           but were not built in the current target. Construct a relation
           from the graph node coordinates so the generated SQL references
           the correct schema instead of rendering "from None". #}
        {% if identifier_node %}
            {% do return(
                api.Relation.create(
                    database=elementary_database,
                    schema=elementary_schema,
                    identifier=identifier_alias,
                )
            ) %}
        {% endif %}
    {%- endif %}
{% endmacro %}
