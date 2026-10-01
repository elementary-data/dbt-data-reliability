{% macro get_group_owner(group_name) %}
    {#
        Resolve a dbt group's owner to a list of owner strings, preferring the
        owner email and falling back to the owner name. The email may be a list
        (dbt-core 1.10+). Returns [] when the group or owner is missing.
    #}
    {% if not group_name %} {% do return([]) %} {% endif %}
    {% for group_node in graph.groups.values() %}
        {% if group_node.get("name") == group_name %}
            {% set owner_dict = elementary.safe_get_with_default(
                group_node, "owner", {}
            ) %}
            {% set owner = owner_dict.get("email") or owner_dict.get("name") %}
            {% if not owner %} {% do return([]) %}
            {% elif owner is string %} {% do return([owner]) %}
            {% else %} {% do return(owner | list) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {% do return([]) %}
{% endmacro %}
