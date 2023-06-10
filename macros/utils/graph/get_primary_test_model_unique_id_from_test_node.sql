{% macro get_primary_test_model_unique_id_from_test_node(test_node_dict) %}
    {%- set test_model_unique_ids = elementary.get_parent_model_unique_ids_from_test_node(test_node_dict) %}
    {%- set test_model_nodes = elementary.get_nodes_by_unique_ids(test_model_unique_ids) %}
    {% set primary_test_model_id = namespace(data=none) %}
    {% if test_model_unique_ids | length == 1 %}
        {# if only one parent model for this test, simply use this model #}
        {% set primary_test_model_id.data = test_model_unique_ids[0] %}
    {% else %}
      {% set test_metadata = elementary.safe_get_with_default(test_node_dict, 'test_metadata', {}) %}
      {% set test_kwargs = elementary.safe_get_with_default(test_metadata, 'kwargs', {}) %}
      {% set test_model_jinja = test_kwargs.get('model') %}
      {% if test_model_jinja %}
        {% set test_model_name_matches = modules.re.findall("ref\(['\"](\w+)['\"]\)", test_model_jinja) %}
        {% if test_model_name_matches | length == 1 %}
          {% set test_model_name = test_model_name_matches[0] %}
          {% for test_model_unique_id in test_model_unique_ids %}
              {% set split_test_model_unique_id = test_model_unique_id.split('.') %}
              {% if split_test_model_unique_id and split_test_model_unique_id | length > 0 %}
                  {% set test_node_model_name = split_test_model_unique_id[-1] %}
                  {% if test_node_model_name == test_model_name %}
                    {% set primary_test_model_id.data = test_model_unique_id %}
                  {% endif %}
              {% endif %}
          {% endfor %}
        {% endif %}
      {% endif %}
    {% endif %}
    {{ return(primary_test_model_id.data) }}
{% endmacro %}