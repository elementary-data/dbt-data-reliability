{% macro get_run_result_dict(run_result) %}
    {% set major, minor, revision = dbt_version.split(".") %}
    {% set major = major | int %}
    {% set minor = minor | int %}
    {% if major < 1 or major == 1 and minor < 8 %}
        {% do return(elementary.dbt_object_to_dict(run_result)) %}
    {% else %}
        {# There's a bug in dbt 1.8 which causes run_result.to_dict to fail on an exception #}
        {% set timing_dicts = [] %}
        {% if run_result.timing %}
            {% for item in run_result.timing %}
                {% do timing_dicts.append(
                    elementary.normalize_timing_dict(
                        elementary.dbt_object_to_dict(item)
                    )
                ) %}
            {% endfor %}
        {% endif %}

        {% do return(
            {
                "status": run_result.status,
                "message": run_result.message,
                "adapter_response": run_result.adapter_response,
                "failures": run_result.failures,
                "execution_time": run_result.execution_time,
                "timing": timing_dicts,
                "node": (
                    elementary.dbt_object_to_dict(run_result.node)
                    if run_result.node
                    else None
                ),
                "thread_id": run_result.thread_id,
            }
        ) %}
    {% endif %}
{% endmacro %}

{# dbt Fusion exposes timing timestamps as datetime objects rather than ISO
   strings; render_value would otherwise insert them as null. #}
{% macro normalize_timing_dict(timing) %}
    {% if timing is not mapping %} {% do return(timing) %} {% endif %}
    {% set normalized = {} %}
    {% for key, value in timing.items() %}
        {% if key in [
            "started_at",
            "completed_at",
        ] and value is not none and value is not string %}
            {% do normalized.update({key: value.isoformat()}) %}
        {% else %} {% do normalized.update({key: value}) %}
        {% endif %}
    {% endfor %}
    {% do return(normalized) %}
{% endmacro %}
