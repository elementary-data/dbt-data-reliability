{% macro is_test_command(command) %}
    {% do return(command in ['test', 'build']) %}
{% endmacro %}

{% macro is_run_command(command) %}
    {% do return(command in ['run', 'build']) %}
{% endmacro %}

{% macro is_docs_command(command) %}
    {% do return(command in ['generate', 'serve']) %}
{% endmacro %}
