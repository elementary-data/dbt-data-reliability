{#
  Inverse of PII protection: when enable_samples_on_show_sample_rows_tags is true,
  samples are hidden by default and only shown when the show_sample_rows tag is present.

  Checks three levels in order: model → test → column (test's target column only).
  Returns true if any level has a matching show_sample_rows tag.

  PII precedence: if disable_samples_on_pii_tags is also enabled and the model
  or column has a PII tag, PII wins and this returns false. A model-level PII
  tag blocks show_sample_rows at every level (model, test, and column).

  All tag matching is case-insensitive (tags are normalized to lowercase).
#}
{% macro should_show_sample_rows(flattened_test) %}
    {% if not elementary.get_config_var("enable_samples_on_show_sample_rows_tags") %}
        {% do return(false) %}
    {% endif %}

    {% set raw_show_tags = elementary.get_config_var("show_sample_rows_tags") %}
    {% if raw_show_tags is string %} {% set show_tags = [raw_show_tags | lower] %}
    {% else %} {% set show_tags = (raw_show_tags or []) | map("lower") | list %}
    {% endif %}

    {#
      Resolve PII tags once upfront. We use `is string` (not `is iterable`) because
      strings are iterable in Jinja — iterating a string gives individual characters.
    #}
    {% set check_pii = elementary.get_config_var("disable_samples_on_pii_tags") %}
    {% if check_pii %}
        {% set raw_pii_tags = elementary.get_config_var("pii_tags") %}
        {% if raw_pii_tags is string %} {% set pii_tags = [raw_pii_tags | lower] %}
        {% else %} {% set pii_tags = (raw_pii_tags or []) | map("lower") | list %}
        {% endif %}
    {% else %} {% set pii_tags = [] %}
    {% endif %}

    {# Model-level: show_sample_rows on the model applies to all its tests #}
    {% set raw_model_tags = elementary.insensitive_get_dict_value(
        flattened_test, "model_tags", []
    ) %}
    {% if raw_model_tags is string %} {% set model_tags = [raw_model_tags | lower] %}
    {% else %} {% set model_tags = (raw_model_tags or []) | map("lower") | list %}
    {% endif %}
    {% if elementary.lists_intersection(model_tags, show_tags) | length > 0 %}
        {# PII on the model takes precedence over show_sample_rows on the same model #}
        {% if check_pii and elementary.lists_intersection(
            model_tags, pii_tags
        ) | length > 0 %}
            {% do return(false) %}
        {% endif %}
        {% do return(true) %}
    {% endif %}

    {# Test-level: show_sample_rows on the test definition itself #}
    {% set raw_test_tags = elementary.insensitive_get_dict_value(
        flattened_test, "tags", []
    ) %}
    {% if raw_test_tags is string %} {% set test_tags = [raw_test_tags | lower] %}
    {% else %} {% set test_tags = (raw_test_tags or []) | map("lower") | list %}
    {% endif %}
    {% if elementary.lists_intersection(test_tags, show_tags) | length > 0 %}
        {# If the model itself is PII-tagged, respect that even for test-level overrides #}
        {% if check_pii and elementary.lists_intersection(
            model_tags, pii_tags
        ) | length > 0 %}
            {% do return(false) %}
        {% endif %}
        {% do return(true) %}
    {% endif %}

    {#
      Column-level: only checks the specific column the test targets (test_column_name),
      not all columns on the model. This avoids showing samples for unrelated columns.
    #}
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
                        {# PII on the column or model takes precedence over show_sample_rows #}
                        {% if check_pii and (
                            elementary.lists_intersection(col_tags, pii_tags)
                            | length
                            > 0
                            or elementary.lists_intersection(
                                model_tags, pii_tags
                            )
                            | length
                            > 0
                        ) %}
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
