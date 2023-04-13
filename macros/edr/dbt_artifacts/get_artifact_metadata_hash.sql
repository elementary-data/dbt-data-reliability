{% macro get_artifact_metadata_hash(artifact) %}
  {% if not local_md5 %}
    {% do return(none) %}
  {% endif %}

  {% set time_excluded_artifact = artifact.copy() %}
  {% do time_excluded_artifact.pop("generated_at") %}
  {% do return(local_md5(tojson(time_excluded_artifact, sort_keys=true))) %}
{% endmacro %}
