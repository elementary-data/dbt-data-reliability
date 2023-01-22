{%- macro upload_dbt_snapshots(should_commit=false, state_hash=none) -%}
    {% set relation = elementary.get_elementary_relation('dbt_snapshots') %}
    {% if execute and relation %}
        {% set snapshots = graph.nodes.values() | selectattr('resource_type', '==', 'snapshot') %}
        {% do elementary.upload_artifacts_to_table(relation, snapshots, elementary.flatten_model, should_commit=should_commit, state_hash=state_hash) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}
