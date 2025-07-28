{% macro run_query(query, lowercase_column_names=True) %}
    {% set query_result = dbt.run_query(elementary.format_query_with_metadata(query)) %}
    {% if lowercase_column_names %}
        {% set lowercased_column_names = {} %}
        {% for column_name in query_result.column_names %}
            {% do lowercased_column_names.update({column_name: column_name.lower()}) %}
        {% endfor %}
        {% set query_result = query_result.rename(lowercased_column_names) %}
    {% endif %}

    {% do return(query_result) %}
{% endmacro %}

{% macro format_query_with_metadata(query) %}
    {% do return(adapter.dispatch('format_query_with_metadata', 'elementary')(query)) %}
{% endmacro %}

{% macro default__format_query_with_metadata(query) %}
    /* --ELEMENTARY-METADATA-- {{ elementary.get_elementary_query_metadata() | tojson }} --END-ELEMENTARY-METADATA-- */
    {{ query }}
{% endmacro %}

{% macro snowflake__format_query_with_metadata(query) %}
    {#- Strip ; from last statement to prevent error in dbt-fusion -#}
    {%- set query = query.strip() -%}
    {%- if query.endswith(';') -%}
        {%- set query = query[:-1] -%}
    {%- endif -%}

    {# Snowflake removes leading comments, so comment is after the statement #}
    {{ query }}
    /* --ELEMENTARY-METADATA-- {{ elementary.get_elementary_query_metadata() | tojson }} --END-ELEMENTARY-METADATA-- */
{% endmacro %}

{% macro get_elementary_query_metadata() %}
    {% set metadata = {
        "invocation_id": invocation_id,
        "command": flags.WHICH
    } %}

    {% if model %}
        {% do metadata.update({
            'package_name': model['package_name'],
            'resource_name': model['name'],
            'resource_type': model['resource_type']
        }) %}
        {% if model.resource_type == 'test' %}
            {% set test_metadata = model.get('test_metadata', {}) %}
            {% do metadata.update({
                'test_short_name': test_metadata.get("name"),
                'test_namespace': test_metadata.get("namespace")
            }) %}
        {% endif %}
    {% endif %}

    {% do return(metadata) %}
{% endmacro %}
