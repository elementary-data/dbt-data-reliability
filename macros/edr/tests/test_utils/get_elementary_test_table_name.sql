{% macro get_elementary_test_table_name() -%}
    {{ return(adapter.dispatch('get_elementary_test_table_name', 'elementary') ()) }}
{%- endmacro %}

{% macro default__get_elementary_test_table_name() %}
    {% set test_node = model %}
    {% set test_hash = test_node.unique_id.split(".")[-1] %}
    {% set test_name = test_node.name %}
    {% do return('test_{}_{}'.format(test_hash, test_name)) %}
{% endmacro %}

{% macro sqlserver__get_elementary_test_table_name() %}
    {% set test_node = model %}
    {% set test_hash = test_node.unique_id.split(".")[-1] %}
    {% set test_name = test_node.name %}
        {%- if 'test_{}_{}'.format(test_hash, test_name)|length > 100 -%}
            {% set test_name = test_name.split("_")[0:4]|join('_') %}
        {%- endif -%}
        {% do return('test_{}_{}'.format(test_hash, test_name)) %}
{% endmacro %}