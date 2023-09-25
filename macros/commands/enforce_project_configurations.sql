{%- macro enforce_project_configurations(enforce_owners=false, enforce_tags=[], enforce_meta_params=[], enforce_config_params=[], exclude_sources=false) -%}
    {%- if execute -%}
        {# enforcing source params #}
        {%- if not exclude_sources -%}
            {% set sources = graph.sources.values() | selectattr('resource_type', '==', 'source') %}
            {% set sources_result = elementary.enforce_configuration(sources, elementary.flatten_source, enforce_owners, enforce_tags, enforce_meta_params, enforce_config_params) %}
        {%- endif -%}

        {# enforcing model params #}
        {% set models = graph.nodes.values() | selectattr('resource_type', '==', 'model') %}
        {% set models_result = elementary.enforce_configuration(models, elementary.flatten_model, enforce_owners, enforce_tags, enforce_meta_params, enforce_config_params)%}

        {%- if models_result or sources_result -%}
            {{ exceptions.raise_compiler_error("Found issues in projdct configurations") }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro -%}

{%- macro get_enforcement_param(flattened_node, enforcement_param_name, enforcement_param_arg_value) -%}
    {% set node_enforcement_param = flattened_node.meta.get(enforcement_param_name) or flattened_node.config.get(enforcement_param_name) or elementary.elementary.get_config_var(enforcement_param_name) or enforcement_param_arg_value %}
    {{- return(node_enforcement_param) -}}
{%- endmacro -%}

{%- macro enforce_configuration(nodes, flatten_callback, enforce_owners, enforce_tags=[], enforce_meta_params=[], enforce_config_params=[]) -%}
    {% set validation_result = {'success': true} %}
    {% for node in nodes -%}
        {% set flattened_node = flatten_callback(node) %}
        {%- if flattened_node.package_name == project_name -%}
            {% set enforce_owners = elementary.get_enforcement_param(flattened_node, 'enforce_owners', enforce_owners) %}
            {% set enforce_tags = elementary.get_enforcement_param(flattened_node, 'enforce_tags', enforce_tags) %}
            {% set enforce_meta_params = elementary.get_enforcement_param(flattened_node, 'enforce_meta_params', enforce_meta_params) %}
            {% set enforce_config_params = elementary.get_enforcement_param(flattened_node, 'enforce_config_params', enforce_config_params) %}

            {%- if enforce_owners and flattened_node.owner | length == 0 -%}
                {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ " does not have an owner") %}
                {% do validation_result.update({'success': false}) %}
            {%- endif -%}

            {%- if enforce_tags | length > 0 -%}
                {%- if flattened_node.tags | length == 0 -%}
                    {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ " does not have required tags") %}
                    {% do validation_result.update({'success': false}) %}
                {%- endif -%}

                {% set flattened_node_tags_set = set(flattened_node.tags) %}
                {% set enforced_node_tags_set = set(enforce_node_tags) %}
                {%- if flattened_node_tags_set.intersect(enforced_node_tags_set) | length == 0 -%}
                    {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ " does not have required tags") %}
                    {% do validation_result.update({'success': false}) %}
                {%- endif -%}
            {%- endif -%}

            {%- if enforce_meta_params | length > 0 -%}
                {%- for meta_param in enforce_meta_params -%}
                    {%- if meta_param not in flattened_node.meta -%}
                        {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ " does not have required meta param " ~ meta_param) %}
                        {% do validation_result.update({'success': false}) %}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}

            {%- if enforce_config_params | length > 0 -%}
                {%- for config_param in enforce_config_params -%}
                    {%- if flattened_node.config is not none -%}
                        {%- if config_param not in flattened_node.config -%}
                            {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ " does not have required config param " ~ config_param) %}
                            {% do validation_result.update({'success': false}) %}
                        {%- endif -%}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
    {{- return(validation_result['success']) -}}
{%- endmacro -%}