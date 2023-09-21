{%- macro enforce_project_configurations(enforce_model_owners=false, enforce_source_owners=false, enforce_model_tags=[], enforce_source_tags=[], enforce_model_meta_params=[], enforce_model_config_params=[], enforce_source_config_params=[]) -%}
    {%- if execute -%}
        {# enforcing source params #}
        {% set sources = graph.sources.values() | selectattr('resource_type', '==', 'source') %}
        {% set sources_result = elementary.enforce_configuration(sources, elementary.flatten_source, enforce_source_owners, enforce_source_tags, enforce_source_meta_params, enforce_source_config_params) %}

        {# enforcing model params #}
        {% set models = graph.nodes.values() | selectattr('resource_type', '==', 'model') %}
        {% set models_result = elementary.enforce_configuration(models, elementary.flatten_model, enforce_model_owners, enforce_model_tags, enforce_model_meta_params, enforce_model_config_params)%}

        {%- if models_result or sources_result -%}
            {{ exceptions.raise_compiler_error("Found issues in projdct configurations") }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro -%}


{%- macro enforce_configuration(nodes, flatten_callback, enforce_owners, enforce_tags=[], enforce_meta_params=[], enforce_config_params=[]) -%}
    {% set validation_result = {'success': true} %}
    {% for node in nodes -%}
        {% set flattened_node = flatten_callback(node) %}
        {%- if enforce_owners and flattened_node.owner | length == 0 -%}
            {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ "does not have an owner") %}
            {% do validation_result.update({'success': false}) %}
        {%- endif -%}

        {%- if enforce_tags | legnth > 0 -%}
            {%- if flattened_node.tags | length == 0 -%}
                {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ "does not have required tags") %}
                {% do validation_result.update({'success': false}) %}
            {%- endif -%}

            {% set flattened_node_tags_set = set(flattened_node.tags) %}
            {% set enforced_node_tags_set = set(enforce_node_tags) %}
            {%- if flattened_node_tags_set.intersect(enforced_node_tags_set) | length == 0 -%}
                {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ "does not have required tags") %}
                {% do validation_result.update({'success': false}) %}
            {%- endif -%}
        {%- endif -%}

        {%- if enforce_meta_params | length > 0 -%}
            {%- for meta_param in enforce_meta_params -%}
                {%- if meta_param not in flattened_node.meta -%}
                    {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ "does not have required meta param " ~ meta_param) %}
                    {% do validation_result.update({'success': false}) %}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}

        {%- if enforce_config_params | length > 0 -%}
            {%- for config_param in enforce_config_params -%}
                {%- if flattened_node.config is not none -%}
                    {%- if config_param not in flattened_node.config -%}
                        {% do elementary.edr_log(node.resource_type ~ " " ~ node.name ~ "does not have required config param " ~ config_param) %}
                        {% do validation_result.update({'success': false}) %}
                    {%- endif -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
    {{- return(validation_result['success']) -}}
{%- endmacro -%}