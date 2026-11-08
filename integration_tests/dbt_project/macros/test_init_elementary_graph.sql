{% macro test_init_elementary_graph(existing) %}
    {#- `existing` is what an earlier cache read can leave on the graph; none means nothing yet -#}
    {% if existing is none %} {% do graph.pop("elementary", none) %}
    {% else %} {% do graph.update({"elementary": existing}) %}
    {% endif %}
    {% do elementary.init_elementary_graph() %}
    {{ return(graph["elementary"]) }}
{% endmacro %}
