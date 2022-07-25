{% macro models_depend_on_macros() %}
    {%- if execute %}
        {%- set models_using_macros = [] %}
        {%- set models = graph.nodes.values() | selectattr('resource_type', '==', 'model') %}
        {%- for model_node in models %}
            {%- set depends_on_macros = model_node.get('depends_on').get('macros') %}
            {%- if depends_on_macros | length > 1 %}
                {%- do models_using_macros.append((model_node.get('name'), depends_on_macros)) -%}
            {%- endif %}
        {%- endfor %}
        {%- for model in models_using_macros %}
            {%- do print(model[0] ~ ' Depends on: '~ model[1]) -%}
        {%- endfor %}
    {%- endif %}
{% endmacro %}