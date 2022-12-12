{% macro upload_dbt_artifacts() %}
  {% if execute and results %}
    {% set artifacts = [
      {"model": "dbt_models", "which": "run", "handler": elementary.upload_dbt_models},
      {"model": "dbt_tests", "which": "test", "handler": elementary.upload_dbt_tests},
      {"model": "dbt_snapshots", "which": "snapshot", "handler": elementary.upload_dbt_snapshots},
      {"model": "dbt_sources", "which": none, "handler": elementary.upload_dbt_sources},
      {"model": "dbt_metrics", "which": none, "handler": elementary.upload_dbt_metrics},
      {"model": "dbt_exposures", "which": none, "handler": elementary.upload_dbt_exposures},
    ] %}
    {% do elementary.debug_log("Uploading dbt artifacts.") %}
    {% for artifact in artifacts %}
      {% if flags.WHICH == "build" or flags.WHICH == artifact.which %}
        {% if not elementary.get_result_node(artifact.model) %}
          {% set relation = elementary.get_elementary_relation(artifact.model) %}
          {% if relation %}
            {% do artifact.handler(should_commit=true, cache=elementary.get_config_var('cache_artifacts')) %}
          {% endif %}
        {% else %}
          {% do elementary.debug_log('[{}] Artifacts already ran.'.format(artifact.model)) %}
        {% endif %}
      {% endif %}
    {% endfor %}
    {% do elementary.debug_log("Uploaded dbt artifacts successfully.") %}
  {% endif %}
{% endmacro %}
