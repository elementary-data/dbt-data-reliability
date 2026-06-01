{% macro upload_source_freshness() %}
    {% set source_freshness_results_relation = elementary.get_elementary_relation(
        "dbt_source_freshness_results"
    ) %}
    {% set source_freshness_results_dicts = [] %}
    {% for result in results %}
        {% set processed_result = elementary.process_freshness_result(result) %}
        {% if processed_result is not none %}
            {% do source_freshness_results_dicts.append(processed_result) %}
        {% endif %}
    {% endfor %}
    {% do elementary.upload_artifacts_to_table(
        source_freshness_results_relation,
        source_freshness_results_dicts,
        elementary.flatten_source_freshness,
        append=True,
        should_commit=True,
    ) %}
{% endmacro %}

{% macro process_freshness_result(result) %}
    {% set result_dict = result.to_dict() %}

    {#
        dbt-core nests the source identifiers under `node` (a full source node),
        while dbt-fusion returns a flat result that mirrors `sources.json`: `node`
        is none and `unique_id` / `criteria` live at the top level. Resolve from
        whichever is populated so both engines upload complete results.
    #}
    {% set node = result_dict.get("node") %}
    {% set has_node = node is not none and node is not undefined %}
    {% set unique_id = (
        node.get("unique_id") if has_node else result_dict.get("unique_id")
    ) %}

    {% if unique_id is none %}
        {# Nothing identifiable to record (e.g. fusion error result with no node). #}
        {% do return(none) %}
    {% endif %}

    {% if result_dict.get("status") == "runtime error" %}
        {% do return(
            {
                "unique_id": unique_id,
                "status": result_dict.get("status"),
                "error": result_dict.get("message") or result_dict.get("error"),
            }
        ) %}
    {% endif %}

    {% set criteria = (
        node.get("freshness", {}) if has_node else result_dict.get("criteria", {})
    ) %}
    {% do return(
        {
            "unique_id": unique_id,
            "status": result_dict.get("status"),
            "max_loaded_at": result_dict.get("max_loaded_at"),
            "snapshotted_at": result_dict.get("snapshotted_at"),
            "max_loaded_at_time_ago_in_s": result_dict.get("age")
            if result_dict.get("age") is not none
            else result_dict.get("max_loaded_at_time_ago_in_s"),
            "criteria": criteria or {},
            "adapter_response": result_dict.get("adapter_response"),
            "timing": result_dict.get("timing"),
            "thread_id": result_dict.get("thread_id"),
            "execution_time": result_dict.get("execution_time"),
        }
    ) %}
{% endmacro %}

{% macro flatten_source_freshness(node_dict) %}
    {% set compile_timing = {} %}
    {% set execute_timing = {} %}
    {% for timing in node_dict.get("timing") or [] %}
        {% if timing["name"] == "compile" %} {% do compile_timing.update(timing) %}
        {% elif timing["name"] == "execute" %} {% do execute_timing.update(timing) %}
        {% endif %}
    {% endfor %}
    {% set metadata_dict = elementary.safe_get_with_default(
        node_dict, "metadata", {}
    ) %}
    {% set criteria_dict = elementary.safe_get_with_default(
        node_dict, "criteria", {}
    ) %}
    {% set source_freshness_invocation_id = metadata_dict.get(
        "invocation_id", invocation_id
    ) %}
    {% set flatten_source_freshness_dict = {
        "source_freshness_execution_id": [
            source_freshness_invocation_id,
            node_dict.get("unique_id"),
        ]
        | join("."),
        "unique_id": node_dict.get("unique_id"),
        "max_loaded_at": node_dict.get("max_loaded_at"),
        "snapshotted_at": node_dict.get("snapshotted_at"),
        "max_loaded_at_time_ago_in_s": node_dict.get(
            "max_loaded_at_time_ago_in_s"
        ),
        "status": node_dict.get("status"),
        "error": node_dict.get("error"),
        "warn_after": criteria_dict.get("warn_after"),
        "error_after": criteria_dict.get("error_after"),
        "filter": criteria_dict.get("filter"),
        "generated_at": elementary.datetime_now_utc_as_string(),
        "invocation_id": source_freshness_invocation_id,
        "compile_started_at": compile_timing.get("started_at"),
        "compile_completed_at": compile_timing.get("completed_at"),
        "execute_started_at": execute_timing.get("started_at"),
        "execute_completed_at": execute_timing.get("completed_at"),
    } %}
    {{ return(flatten_source_freshness_dict) }}
{% endmacro %}
