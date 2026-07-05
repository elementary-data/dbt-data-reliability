{% macro get_group_owner(group_name) %}
    {#
        Resolve a dbt group's owner to a single string, preferring the owner email
        and falling back to the owner name. Mirrors how dbt_groups exposes
        owner_email / owner_name. Returns none when the group or owner is missing.
    #}
    {% if not group_name %} {% do return(none) %} {% endif %}
    {% for group_node in graph.groups.values() %}
        {% if group_node.get("resource_type") == "group" and group_node.get(
            "name"
        ) == group_name %}
            {% set owner_dict = elementary.safe_get_with_default(
                group_node, "owner", {}
            ) %}
            {% do return(owner_dict.get("email") or owner_dict.get("name")) %}
        {% endif %}
    {% endfor %}
    {% do return(none) %}
{% endmacro %}
