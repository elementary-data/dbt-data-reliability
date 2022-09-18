{% macro get_test_name_in_graph() %}
    {% set test_name = model.name %}
    {% set test_alias = model.alias %}
    {% set test_unique_id = model.unique_id %}
    {# Currently dbt allows multiple tests with the same custom name. #}
    {# In that case we want to use the unique id of the test instead so we won't use the same tests tables for different tests. #}
    {% set test_name_in_graph = test_name if test_name != test_alias else test_unique_id | replace(".", "_") %}
    {{ return(test_name_in_graph) }}
{% endmacro %}
