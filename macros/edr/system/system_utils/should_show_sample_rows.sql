{% macro should_show_sample_rows(flattened_test) %}
    {% if not elementary.get_config_var("enable_samples_on_show_sample_rows_tags") %}
        {% do return(false) %}
    {% endif %}

    {% set raw_show_tags = elementary.get_config_var("show_sample_rows_tags") %}
    {% if raw_show_tags is string %} {% set show_tags = [raw_show_tags | lower] %}
    {% else %} {% set show_tags = (raw_show_tags or []) | map("lower") | list %}
    {% endif %}

    {# Resolve PII tags once — only relevant when PII hiding is also enabled #}
    {% set check_pii = elementary.get_config_var("disable_samples_on_pii_tags") %}
    {% if check_pii %}
        {% set raw_pii_tags = elementary.get_config_var("pii_tags") %}
        {% if raw_pii_tags is string %} {% set pii_tags = [raw_pii_tags | lower] %}
        {% else %} {% set pii_tags = (raw_pii_tags or []) | map("lower") | list %}
        {% endif %}
    {% else %} {% set pii_tags = [] %}
    {% endif %}

    {# Model-level check #}
    {% set raw_model_tags = elementary.insensitive_get_dict_value(
        flattened_test, "model_tags", []
    ) %}
    {% if raw_model_tags is string %} {% set model_tags = [raw_model_tags | lower] %}
    {% else %} {% set model_tags = (raw_model_tags or []) | map("lower") | list %}
    {% endif %}
    {% if elementary.lists_intersection(model_tags, show_tags) | length > 0 %}
        {% if check_pii and elementary.lists_intersection(
            model_tags, pii_tags
        ) | length > 0 %}
            {% do return(false) %}
        {% endif %}
        {% do return(true) %}
    {% endif %}

    {# Test-level check #}
    {% set raw_test_tags = elementary.insensitive_get_dict_value(
        flattened_test, "tags", []
    ) %}
    {% if raw_test_tags is string %} {% set test_tags = [raw_test_tags | lower] %}
    {% else %} {% set test_tags = (raw_test_tags or []) | map("lower") | list %}
    {% endif %}
    {% if elementary.lists_intersection(test_tags, show_tags) | length > 0 %}
        {% if check_pii and elementary.lists_intersection(
            model_tags, pii_tags
        ) | length > 0 %}
            {% do return(false) %}
        {% endif %}
        {% do return(true) %}
    {% endif %}

    {# Column-level check: only the test's target column #}
    {% set test_column_name = elementary.insensitive_get_dict_value(
        flattened_test, "test_column_name"
    ) %}
    {% if test_column_name %}
        {% set parent_model_unique_id = elementary.insensitive_get_dict_value(
            flattened_test, "parent_model_unique_id"
        ) %}
        {% set parent_model = elementary.get_node(parent_model_unique_id) %}
        {% if parent_model %}
            {% set column_nodes = parent_model.get("columns", {}) %}
            {% for col_name, col_node in column_nodes.items() %}
                {% if col_name | lower == test_column_name | lower %}
                    {% set col_tags = elementary.get_column_tags(col_node) %}
                    {% if elementary.lists_intersection(
                        col_tags, show_tags
                    ) | length > 0 %}
                        {% if check_pii and elementary.lists_intersection(
                            col_tags, pii_tags
                        ) | length > 0 %}
                            {% do return(false) %}
                        {% endif %}
                        {% do return(true) %}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}

    {% do return(false) %}
{% endmacro %}
