{%- macro get_model_database_and_schema(package_name, model_name) -%}
    {% if execute %}
        {% set nodes_in_package = graph.nodes.values()
                                 | selectattr("resource_type", "==", "model")
                                 | selectattr("package_name", "==", package_name) %}
        {% if nodes_in_package %}
            {%- for node_in_package in nodes_in_package -%}
                {%- if node_in_package.name | lower == model_name | lower -%}
                    {{ return([node_in_package.database, node_in_package.schema]) }}
                {%- endif -%}
            {%- endfor -%}
        {% endif %}
    {% endif %}
    {{ return([none, none]) }}
{%- endmacro -%}