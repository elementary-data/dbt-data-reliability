{% macro run_query_macro_on_list(param_list, query_macro) %}

    {% for param in param_list %}
        {{ query_macro(param) }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}

{% endmacro %}