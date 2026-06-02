{% macro get_indexes_for_model(model_name, base_indexes) %}
    {%- if target.type != "postgres" -%} {{ return([]) }} {%- endif -%}

    {% set extra_indexes_config = var("elementary_extra_indexes", {}) %}
    {% set extra_indexes = extra_indexes_config.get(model_name, []) %}

    {% set seen_column_sets = [] %}
    {% set merged_indexes = [] %}

    {% for idx in base_indexes %}
        {% set sorted_cols = idx.columns | sort | join(",") %}
        {% if sorted_cols not in seen_column_sets %}
            {% do seen_column_sets.append(sorted_cols) %}
            {% do merged_indexes.append(idx) %}
        {% endif %}
    {% endfor %}

    {% for idx in extra_indexes %}
        {% set sorted_cols = idx.columns | sort | join(",") %}
        {% if sorted_cols not in seen_column_sets %}
            {% do seen_column_sets.append(sorted_cols) %}
            {% do merged_indexes.append(idx) %}
        {% endif %}
    {% endfor %}

    {{ return(merged_indexes) }}
{% endmacro %}
