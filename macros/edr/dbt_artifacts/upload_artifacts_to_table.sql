{% macro upload_artifacts_to_table(table_relation, artifacts, flatten_artifact_callback, should_commit=False, cache=False) %}
    {% set flatten_artifact_dicts = [] %}
    {% for artifact in artifacts %}
        {% set flatten_artifact_dict = flatten_artifact_callback(artifact) %}
        {% if flatten_artifact_dict is not none %}
            {% do flatten_artifact_dicts.append(flatten_artifact_dict) %}
        {% endif %}
    {% endfor %}
    {% if cache %}
      {% set time_excluded_artifacts = elementary.get_time_excluded_artifacts(flatten_artifact_dicts) %}
      {% if elementary.is_on_run_end() %}
        {% set cached_artifacts = elementary.get_cached_artifacts(table_relation) %}
        {% if cached_artifacts == time_excluded_artifacts %}
          {{ elementary.debug_log("[{}] Artifacts were not changed. Skipping upload.".format(table_relation.identifier)) }}
          {{ return(none) }}
        {% else %}
          {% do dbt.truncate_relation(table_relation) %}
        {% endif %}
      {% endif %}
    {% endif %}
    {% do elementary.insert_rows(table_relation, flatten_artifact_dicts, should_commit, elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    -- remove empty rows
    {% do elementary.remove_empty_rows(table_relation) %}
    {%- if should_commit -%}
        {% do adapter.commit() %}
    {%- endif -%}
    {% if cache %}
      {% do elementary.cache_artifacts(table_relation, time_excluded_artifacts) %}
    {% endif %}
{% endmacro %}


{% macro get_cached_artifacts(table_relation) %}
  {% set filename = table_relation.quote(false, false, false) | string | lower %}
  {% set cached_artifacts_path = elementary.get_target_path(filename) %}
  {% if cached_artifacts_path.exists() %}
    {{ return(fromjson(cached_artifacts_path.read_text())) }}
  {% endif %}
  {{ return(none) }}
{% endmacro %}

{% macro cache_artifacts(table_relation, time_excluded_artifacts) %}
  {% set filename = table_relation.quote(false, false, false) | string | lower %}
  {% set cached_artifacts_path = elementary.get_target_path(filename) %}
  {% do cached_artifacts_path.write_text(tojson(time_excluded_artifacts)) %}
{% endmacro %}

{% macro get_time_excluded_artifacts(artifacts) %}
  {% set time_excluded_artifacts = [] %}
  {% for artifact in artifacts %}
    {% set artifact_copy = artifact.copy() %}
    {% do artifact_copy.pop('generated_at') %}
    {% do time_excluded_artifacts.append(artifact_copy) %}
  {% endfor %}
  {{ return(time_excluded_artifacts) }}
{% endmacro %}
