{% macro get_indexes_for_model(model_name, base_indexes) %}
    {# dbt's indexes config is Postgres-only. Returns [] for all other targets. #}
    {%- if target.type != "postgres" -%} {{ return([]) }} {%- endif -%}

    {% set extra_indexes_config = elementary.get_config_var(
        "elementary_extra_indexes"
    ) %}
    {% set extra_indexes = extra_indexes_config.get(model_name, []) %}

    {# Dedup by sorted column set only. Base indexes take precedence on conflicts. #}
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
        {% if idx.columns is not defined %}
            {{
                exceptions.raise_compiler_error(
                    "elementary_extra_indexes entry for '"
                    ~ model_name
                    ~ "' is missing 'columns': "
                    ~ idx
                )
            }}
        {% endif %}
        {% set sorted_cols = idx.columns | sort | join(",") %}
        {% if sorted_cols not in seen_column_sets %}
            {% do seen_column_sets.append(sorted_cols) %}
            {% do merged_indexes.append(idx) %}
        {% endif %}
    {% endfor %}

    {{ return(merged_indexes) }}
{% endmacro %}
