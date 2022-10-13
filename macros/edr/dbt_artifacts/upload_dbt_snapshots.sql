{%- macro upload_dbt_snapshots() -%}
    {% set identifier = 'dbt_snapshots' %}
    {% if results and elementary.get_result_node('model.elementary.%s' % identifier) %}
      {{ elementary.debug_log('[%s] Artifacts already ran.' % identifier) }}
      {{ return(none) }}
    {% endif %}

    {% set relation = elementary.get_elementary_relation(identifier) %}
    {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
    {% if execute and not edr_cli_run %}
        {% set snapshots = graph.nodes.values() | selectattr('resource_type', '==', 'snapshot') %}
        {% do elementary.upload_csv_artifacts_to_table(relation, snapshots, elementary.get_flatten_model_callback()) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}
