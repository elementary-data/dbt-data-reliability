{% macro union_macro_queries(param_list, query_macro) %}
    {% for param in param_list %}
        ({{ query_macro(param) }})
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
{% endmacro %}
