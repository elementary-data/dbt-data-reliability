{% macro get_model_relation_for_test(test_model, test_node) %}
    {# If the test model is not a string, then return as-is #}
    {% if test_model is not string %}
        {% do return(test_model) %}
    {% endif %}

    {# If the test depends on a single model ID, try to get the relation from it #}
    {% set depends_on_node_ids = test_node.get("depends_on", {}).get("nodes") %}
    {% if depends_on_node_ids and depends_on_node_ids | length == 1 %}
        {% set node = elementary.get_node(depends_on_node_ids[0]) %}
        {% set relation = elementary.get_relation_from_node(node) %}
        {% if relation %}
            {% do return(relation) %}
        {% endif %}
    {% endif %}

    {# Test model is a string, this might mean that a "where" parameter was passed to the test.
       In the heuristic below we rely on the fact that in this case the model jinja will have
       a very specific structure (see the "build_model_str" function in dbt-core) #}

    {% set test_metadata = elementary.safe_get_with_default(test_node, 'test_metadata', {}) %}
    {% set test_kwargs = elementary.safe_get_with_default(test_metadata, 'kwargs', {}) %}
    {% set test_model_jinja = test_kwargs.get('model') %}

    {% if not test_model_jinja %}
        {% do return(none) %}
    {% endif %}

    {# Try to match ref #}
    {% set match = modules.re.match(
        "{{ get_where_subquery\(ref\('(?P<model_name>.+)'\)\) }}",
        test_model_jinja
    ) %}
    {% if match %}
        {% set group_dict = match.groupdict() %}
        {% set model_name = group_dict["model_name"] %}
        {% do return(ref(model_name)) %}
    {% endif %}

    {# Try to match source #}
    {% set match = modules.re.match(
        "{{ get_where_subquery\(source\('(?P<source_name>.+)', '(?P<table_name>.+)'\)\) }}",
        test_model_jinja
    ) %}
    {% if match %}
        {% set group_dict = match.groupdict() %}
        {% set source_name = group_dict["source_name"] %}
        {% set table_name = group_dict["table_name"] %}
        {% do return(source(source_name, table_name)) %}
    {% endif %}

    {# If we got here, then probably "ref" or "source" have been overridden with a string query,
       which right now we cannot handle #}
    {% do return(none) %}
{% endmacro %}