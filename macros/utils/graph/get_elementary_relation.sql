{% macro get_elementary_relation(identifier, allow_deferred_fallback=false) %}
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
        {# Relation not found in the target schema. Under dbt deferral
           (--favor-state / --defer) the Elementary models may exist only
           in the deferred (e.g. prod) schema and not in the current
           target. Callers that render the relation into test SQL opt in
           (allow_deferred_fallback=true) to a relation constructed from
           the graph node coordinates, so the test SQL references the
           correct schema instead of rendering "from None". Callers that
           only check whether the relation exists (e.g. the on-run-end
           artifact uploads) keep getting none, so a run against a target
           whose Elementary schema was never created skips the upload
           instead of failing. #}
        {% if not allow_deferred_fallback %} {% do return(none) %} {% endif %}
        {% set is_defer = (
            (
                invocation_args_dict.get("defer", false)
                or invocation_args_dict.get("favor_state", false)
            )
            if invocation_args_dict
            else false
        ) %}
        {% if identifier_node and is_defer %}
            {% do return(
                api.Relation.create(
                    database=elementary_database,
                    schema=elementary_schema,
                    identifier=identifier_alias,
                )
            ) %}
        {% endif %}
        {% do return(none) %}
    {%- endif %}
{% endmacro %}
