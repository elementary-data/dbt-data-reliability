{%- macro enforce_project_configurations(enforce_owners=true, enforce_tags=false, enforce_description=false, required_meta_keys=none, required_config_keys=none, include_sources=true) -%}
    {% if not required_meta_keys %}
        {% set required_meta_keys = [] %}
    {% endif %}
    {% if not required_config_keys %}
        {% set required_config_keys = [] %}
    {% endif %}

    {# enforcing source params #}
    {%- if include_sources -%}
        {% set sources = graph.sources.values() | selectattr('resource_type', '==', 'source') %}
        {% set sources_passed = elementary.enforce_configuration(sources, elementary.flatten_source, enforce_owners, enforce_tags, enforce_description, required_meta_keys, required_config_keys) %}
    {%- endif -%}

    {# enforcing model params #}
    {% set models = graph.nodes.values() | selectattr('resource_type', '==', 'model') %}
    {% set models_passed = elementary.enforce_configuration(models, elementary.flatten_model, enforce_owners, enforce_tags, enforce_description, required_meta_keys, required_config_keys) %}

    {%- if not models_passed or (include_sources and not sources_passed) -%}
        {{ exceptions.raise_compiler_error("Found issues in project configurations") }}
    {%- endif -%}
{%- endmacro -%}

{%- macro get_enforcement_param(node, enforcement_param_name, enforcement_param_arg_value) -%}
    {% do return(node.meta.get(enforcement_param_name) or node.config.get(enforcement_param_name) or elementary.get_config_var(enforcement_param_name) or enforcement_param_arg_value) %}
{%- endmacro -%}

{%- macro enforce_configuration(nodes, flatten_callback, enforce_owners, enforce_tags, enforce_description, required_meta_keys, required_config_keys) -%}
    {% set validation_result = {'success': true} %}
    {% for node in nodes -%}
        {% set flattened_node = flatten_callback(node) %}
        {%- if flattened_node.package_name == project_name -%}
            {% set enforce_owners = elementary.get_enforcement_param(node, 'enforce_owners', enforce_owners) %}
            {% set enforce_tags = elementary.get_enforcement_param(node, 'enforce_tags', enforce_tags) %}
            {% set enforce_description = elementary.get_enforcement_param(node, 'enforce_description', enforce_description) %}
            {% set required_meta_keys = elementary.get_enforcement_param(node, 'required_meta_keys', required_meta_keys) %}
            {% set required_config_keys = elementary.get_enforcement_param(node, 'required_config_keys', required_config_keys) %}

            {%- if enforce_owners and flattened_node.owner | length == 0 -%}
                {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ " does not have an owner") %}
                {% do validation_result.update({'success': false}) %}
            {%- endif -%}

            {%- if enforce_tags and flattened_node.tags | length == 0 -%}
                {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ " does not have tags") %}
                {% do validation_result.update({'success': false}) %}
            {%- endif -%}

            {%- if enforce_description and not flattened_node.description -%}
                {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ " does not have a description") %}
                {% do validation_result.update({'success': false}) %}
            {%- endif -%}

            {%- if required_meta_keys | length > 0 -%}
                {%- for meta_param in required_meta_keys -%}
                    {%- if meta_param not in flattened_node.meta -%}
                        {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ " does not have required meta param " ~ meta_param) %}
                        {% do validation_result.update({'success': false}) %}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}

            {%- if required_config_keys | length > 0 -%}
                {%- for config_param in required_config_keys -%}
                    {# flattened node doesn't have a config yet, using node instead #}
                    {% set config_dict = elementary.safe_get_with_default(node, 'config', {}) %}
                    {%- if config_dict is not none -%}
                        {%- if config_param not in config_dict -%}
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
