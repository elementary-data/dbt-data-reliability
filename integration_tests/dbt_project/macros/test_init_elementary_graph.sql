{% macro test_init_elementary_graph(existing) %}
    {#- What a cache read before the on-run-start hook can leave on the graph -#}
    {% do graph.update({"elementary": existing}) %}
    {% do elementary.init_elementary_graph() %}
    {{ return(graph["elementary"]) }}
{% endmacro %}
