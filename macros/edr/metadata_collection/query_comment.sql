{% macro query_comment(node) %}
    {%- set query_comment_string = elementary.get_config_var('query_comment_string') %}
    {%- set comment_dict = {} -%}
    {%- do comment_dict.update(
        profile_name=target.get('profile_name'),
        target_name=target.get('target_name'),
    ) -%}
    {%- if node is not none -%}
      {%- do comment_dict.update(
        node_unique_id=node.unique_id,
        resource_type=node.resource_type,
        package_name=node.package_name,
        identifier=node.identifier
      ) -%}
    {% else %}
      {%- do comment_dict.update(node_id='internal') -%}
    {%- endif -%}
    {% do return(query_comment_string ~ tojson(comment_dict) ~ query_comment_string) %}
{% endmacro %}