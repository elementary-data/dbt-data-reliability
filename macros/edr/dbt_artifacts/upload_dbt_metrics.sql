{%- macro upload_dbt_metrics() -%}
    {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
    {% if execute and not edr_cli_run %}
        {% set nodes = graph.metrics.values() | selectattr('resource_type', '==', 'metric') %}
        {% set flatten_node_macro = context['elementary']['flatten_metric'] %}
        {% do elementary.insert_nodes_to_table(this, nodes, flatten_node_macro) %}
        {% do adapter.commit() %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}