{%- macro upload_dbt_exposures() -%}
    {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
    {% if execute and not edr_cli_run %}
        {% set nodes = graph.exposures.values() | selectattr('resource_type', '==', 'exposure') %}
        {% set flatten_node_macro = context['elementary']['flatten_exposure'] %}
        {% do elementary.insert_nodes_to_table(this, nodes, flatten_node_macro) %}
        {% do adapter.commit() %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}
