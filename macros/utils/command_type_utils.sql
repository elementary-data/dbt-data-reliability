{% macro is_test_command() %}
    {% do return(flags.WHICH in ['test', 'build', 'retry']) %}
{% endmacro %}

{% macro is_run_command() %}
    {% do return(flags.WHICH in ['run', 'build', 'retry']) %}
{% endmacro %}

{% macro is_docs_command() %}
    {% do return(flags.WHICH in ['generate', 'serve']) %}
{% endmacro %}
