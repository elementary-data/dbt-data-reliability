{% macro upload_dbt_artifacts(results) %}
        {% do elementary.edr_log("Deprecated - Please remove the call to elementary.upload_dbt_artifacts() on your on-run-end hook as it happens automatically now.") %}
{% endmacro %}
