{% macro source_freshness_coverage() %}
    {%- if execute %}
        {%- set sources_with_freshness = [] %}
        {%- set sources_missing_freshness = [] %}
        {%- set sources_dict = graph.sources.values() | selectattr('resource_type', '==', 'source') %}
        {%- for source_node in sources_dict %}
            {%- set source = {
               'unique_id': source_node.get('unique_id'),
               'freshness': source_node.get('freshness')
            }
            %}
            {%- if source_node.get('freshness').get('warn_after').get('count') %}
                {%- do sources_with_freshness.append(source) -%}
            {%- else %}
                {%- do sources_missing_freshness.append(source) -%}
            {%- endif %}
        {%- endfor %}
        {% do print('Sources with freshness test:') %}
        {%- for source in sources_with_freshness %}
            {%- do print(source.get('unique_id') ~' : '~ source.get('freshness')) -%}
        {%- endfor %}
        {% do print('Sources missing freshness test:') %}
        {%- for source in sources_missing_freshness %}
            {%- do print(source.get('unique_id')) -%}
        {%- endfor %}
    {%- endif %}
{% endmacro %}