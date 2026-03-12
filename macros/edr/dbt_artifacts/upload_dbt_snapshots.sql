{%- macro upload_dbt_snapshots(should_commit=false, metadata_hashes=none) -%}
    {% set relation = elementary.get_elementary_relation("dbt_snapshots") %}
    {% if execute and relation %}
        {% set snapshots = elementary.filter_to_current_project_if_needed(
            graph.nodes.values()
            | selectattr("resource_type", "==", "snapshot")
        ) %}
        {% do elementary.upload_artifacts_to_table(
            relation,
            snapshots,
            elementary.flatten_model,
            should_commit=should_commit,
            metadata_hashes=metadata_hashes,
        ) %}
    {%- endif -%}
    {{- return("") -}}
{%- endmacro -%}
