{% macro make_authorized_view() %}
    {% if target.type != "bigquery" %}
        {% do return('') %}
    {% endif %}

    {% set configured_schemas = elementary.get_configured_schemas_from_graph() %}
    {% for configured_schema in configured_schemas %}
        {# TODO: Check if this can fail, if so, attempt to query beforehand using `can_query_relation`. #}
        {% do adapter.grant_access_to(this, "view", None, {"project": configured_schema[0], "dataset": configured_schema[1]}) %}
    {% endfor %}
{% endmacro %}
