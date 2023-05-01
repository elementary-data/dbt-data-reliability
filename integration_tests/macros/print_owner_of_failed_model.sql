{% macro dbt_run_end(run_results) %}
    {% set failed_models = [] %}

    {% for result in run_results %}
        {{ debug() }}
        {% if result.status == "fail" %}
            {% set model_name = result.node.fqn[-1] %}
            {% set failed_models = failed_models + [model_name] %}
        {% endif %}
    {% endfor %}

    {% if failed_models %}
        {% set model_count = failed_models | length %}
        {% if model_count == 1 %}
            {{ log("1 model failed tests: " ~ failed_models[0], info=True) }}
        {% else %}
            {{ log(model_count ~ " models failed tests: " ~ failed_models | join(", "), info=True) }}
        {% endif %}
    {% else %}
        {{ log("All models passed tests!", info=True) }}
    {% endif %}
{% endmacro %}

