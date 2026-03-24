{% macro is_show_sample_rows_table(flattened_test) %}
    {% if not elementary.get_config_var("enable_samples_on_show_sample_rows_tags") %}
        {% do return(false) %}
    {% endif %}

    {% set raw_show_tags = elementary.get_config_var("show_sample_rows_tags") %}
    {% if raw_show_tags is string %} {% set show_tags = [raw_show_tags | lower] %}
    {% else %} {% set show_tags = (raw_show_tags or []) | map("lower") | list %}
    {% endif %}

    {% set raw_model_tags = elementary.insensitive_get_dict_value(
        flattened_test, "model_tags", []
    ) %}
    {% set model_tags = (
        (raw_model_tags if raw_model_tags is iterable else [raw_model_tags])
        | map("lower")
        | list
    ) %}

    {# PII takes precedence over show_sample_rows, but only when PII hiding is enabled #}
    {% if elementary.get_config_var("disable_samples_on_pii_tags") %}
        {% set raw_pii_tags = elementary.get_config_var("pii_tags") %}
        {% if raw_pii_tags is string %} {% set pii_tags = [raw_pii_tags | lower] %}
        {% else %} {% set pii_tags = (raw_pii_tags or []) | map("lower") | list %}
        {% endif %}
        {% if elementary.lists_intersection(model_tags, pii_tags) | length > 0 %}
            {% do return(false) %}
        {% endif %}
    {% endif %}

    {% set intersection = elementary.lists_intersection(model_tags, show_tags) %}
    {% do return(intersection | length > 0) %}
{% endmacro %}
