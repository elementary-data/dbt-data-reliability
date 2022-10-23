{%- macro upload_dbt_snapshots() -%}
    {% set relation = elementary.get_elementary_relation('dbt_snapshots') %}
    {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
    {% if execute and relation and not edr_cli_run %}
        {% set snapshots = graph.nodes.values() | selectattr('resource_type', '==', 'snapshot') %}
        {% do elementary.upload_artifacts_to_table(relation, snapshots, elementary.flatten_model) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}
