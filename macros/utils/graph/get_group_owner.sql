{% macro get_group_owner(group_name) %}
    {#
        Resolve a dbt group's owner to a flat list of owner strings, preferring
        the owner email and falling back to the owner name. The email may be a
        comma-separated string or a list. Returns [] when the group or owner is
        missing.
    #}
    {% if not group_name %} {% do return([]) %} {% endif %}
    {% for group_node in graph.groups.values() %}
        {% if group_node.get("name") == group_name %}
            {% set owner_dict = elementary.safe_get_with_default(
                group_node, "owner", {}
            ) %}
            {% set owners = elementary.normalize_owners(owner_dict.get("email")) %}
            {% if not owners and owner_dict.get("name") %}
                {% do owners.append(owner_dict.get("name")) %}
            {% endif %}
            {% do return(owners) %}
        {% endif %}
    {% endfor %}
    {% do return([]) %}
{% endmacro %}
