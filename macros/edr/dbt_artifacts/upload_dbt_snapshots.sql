{%- macro upload_dbt_snapshots(should_commit=false) -%}
    {% set relation = elementary.get_elementary_relation('dbt_snapshots') %}
    {% if execute and relation %}
        {% set snapshots = graph.nodes.values() | selectattr('resource_type', '==', 'snapshot') %}
        {% do elementary.upload_artifacts_to_table(relation, snapshots, elementary.flatten_model, truncate_if_on_run_end=true, should_commit=should_commit) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}
