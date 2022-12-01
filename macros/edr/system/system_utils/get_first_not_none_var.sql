{% macro get_first_env_var(var_names) %}
    {% for var_name in var_names %}
        {% set value = env_var(var_name, "") %}
        {% if value %}
            {{ return(value) }}
        {% endif %}
    {% endfor %}
    {{ return(none) }}
{% endmacro %}
