{% macro is_show_sample_rows_table(flattened_test) %}
    {% if not elementary.get_config_var("enable_samples_on_show_sample_rows_tags") %}
        {% do return(false) %}
    {% endif %}

    {% set raw_show_tags = elementary.get_config_var("show_sample_rows_tags") %}
    {% set show_tags = (
        (raw_show_tags if raw_show_tags is iterable else [raw_show_tags])
        | map("lower")
        | list
    ) %}

    {% set raw_model_tags = elementary.insensitive_get_dict_value(
        flattened_test, "model_tags", []
    ) %}
    {% set model_tags = (
        (raw_model_tags if raw_model_tags is iterable else [raw_model_tags])
        | map("lower")
        | list
    ) %}

    {% set intersection = elementary.lists_intersection(model_tags, show_tags) %}
    {% do return(intersection | length > 0) %}
{% endmacro %}
