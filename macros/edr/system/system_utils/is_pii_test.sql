{#
  Complements is_pii_table (model-level) and should_disable_sampling_for_pii
  (column-level) by adding test-level PII tag support. A test tagged with a PII
  tag will have its samples disabled, consistent with the other two levels.
#}
{% macro is_pii_test(flattened_test) %}
    {% if not elementary.get_config_var("disable_samples_on_pii_tags") %}
        {% do return(false) %}
    {% endif %}

    {% set raw_pii_tags = elementary.get_config_var("pii_tags") %}
    {% if raw_pii_tags is string %} {% set pii_tags = [raw_pii_tags | lower] %}
    {% else %} {% set pii_tags = (raw_pii_tags or []) | map("lower") | list %}
    {% endif %}

    {% set raw_test_tags = elementary.insensitive_get_dict_value(
        flattened_test, "tags", []
    ) %}
    {% if raw_test_tags is string %} {% set test_tags = [raw_test_tags | lower] %}
    {% else %} {% set test_tags = (raw_test_tags or []) | map("lower") | list %}
    {% endif %}

    {% do return(elementary.lists_intersection(test_tags, pii_tags) | length > 0) %}
{% endmacro %}
