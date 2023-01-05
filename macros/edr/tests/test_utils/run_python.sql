{% macro run_python(graph_node, code) %}
  {% set macro_stack = context_macro_stack.call_stack %}
  {% set macro_stack_copy = macro_stack.copy() %}
  {% do macro_stack.clear() %}
  {% do macro_stack.extend([["materialization"], "macro.dbt.statement"]) %}
  {% do submit_python_job(graph_node, code) %}
  {% do macro_stack.clear() %}
  {% do macro_stack.extend(macro_stack_copy) %}
{% endmacro %}
