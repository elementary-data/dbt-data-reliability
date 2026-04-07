{% macro get_column_tags(column_node) %}
    {% set _tags_sources = [
        column_node.get("tags", []),
        column_node.get("config", {}).get("tags", []),
        column_node.get("meta", {}).get("tags", []),
        column_node.get("config", {}).get("meta", {}).get("tags", []),
    ] %}

    {% set all_column_tags = [] %}
    {% for src in _tags_sources %}
        {% set tags_list = src if src is iterable and not (src is string) else [src] %}
        {% do all_column_tags.extend(tags_list) %}
    {% endfor %}

    {% do return(all_column_tags | map("lower") | unique | list) %}
{% endmacro %}

{% macro get_pii_columns_from_parent_model(flattened_test) %}
    {% set pii_columns = [] %}

    {% if not elementary.get_config_var("disable_samples_on_pii_tags") %}
        {% do return(pii_columns) %}
    {% endif %}

    {% set parent_model_unique_id = elementary.insensitive_get_dict_value(
        flattened_test, "parent_model_unique_id"
    ) %}
    {% set parent_model = elementary.get_node(parent_model_unique_id) %}

    {% if not parent_model %} {% do return(pii_columns) %} {% endif %}

    {% set raw_pii_tags = elementary.get_config_var("pii_tags") %}
    {% if raw_pii_tags is string %} {% set pii_tags = [raw_pii_tags | lower] %}
    {% else %} {% set pii_tags = (raw_pii_tags or []) | map("lower") | list %}
    {% endif %}

    {# Check individual columns for PII tags #}
    {% set column_nodes = parent_model.get("columns") %}
    {% if not column_nodes %} {% do return(pii_columns) %} {% endif %}

    {#
      A column tagged show_sample_rows (without pii) should still appear in samples
      even when disable_samples_on_pii_tags is active — it is intentionally opted in.
      We only skip it from the PII columns list if it does NOT also carry a PII tag,
      since PII always takes precedence over show_sample_rows.
    #}
    {% set enable_show_tags = elementary.get_config_var(
        "enable_samples_on_show_sample_rows_tags"
    ) %}
    {% set raw_show_tags = elementary.get_config_var("show_sample_rows_tags") %}
    {% if raw_show_tags is string %} {% set show_tags = [raw_show_tags | lower] %}
    {% else %} {% set show_tags = (raw_show_tags or []) | map("lower") | list %}
    {% endif %}

    {% for column_node in column_nodes.values() %}
        {% set all_column_tags_lower = elementary.get_column_tags(column_node) %}

        {# Skip column from PII list only if show_sample_rows is set and pii is not #}
        {% set has_show_tag = enable_show_tags and (
            elementary.lists_intersection(all_column_tags_lower, show_tags)
            | length
            > 0
        ) %}
        {% set has_pii_tag = (
            elementary.lists_intersection(all_column_tags_lower, pii_tags)
            | length
            > 0
        ) %}
        {% if has_show_tag and not has_pii_tag %} {% continue %} {% endif %}

        {% for pii_tag in pii_tags %}
            {% if pii_tag in all_column_tags_lower %}
                {% do pii_columns.append(column_node.get("name")) %} {% break %}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {% do return(pii_columns) %}
{% endmacro %}
