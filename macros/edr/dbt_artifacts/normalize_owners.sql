{% macro normalize_owners(value) %}
    {#
        Normalize an owner value into a flat list of non-empty, trimmed strings.
        Strings are split on ",", lists are flattened recursively, and other
        scalars are stringified. Returns [] for none or a mapping.
    #}
    {% set owners = [] %}
    {% if value is none or value is mapping %}
    {% elif value is string %}
        {% for owner in value.split(",") %}
            {% if owner | trim %} {% do owners.append(owner | trim) %} {% endif %}
        {% endfor %}
    {% elif value is iterable %}
        {% for item in value %}
            {% do owners.extend(elementary.normalize_owners(item)) %}
        {% endfor %}
    {% else %} {% do owners.append(value | string) %}
    {% endif %}
    {% do return(owners) %}
{% endmacro %}
