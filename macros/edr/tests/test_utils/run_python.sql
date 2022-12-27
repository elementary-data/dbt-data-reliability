{% macro run_python(graph_node, code) %}
  {% set macro_stack = context_macro_stack.call_stack %}
  {% set macro_stack_copy = macro_stack.copy() %}
  {% do macro_stack.clear() %}
  {% do macro_stack.extend([["materialization"], "macro.dbt.statement"]) %}

  {% set try_finally_block -%}
try:
    submit_python_job(graph_node, code)
finally:
    macro_stack.clear()
    macro_stack.extend(macro_stack_copy)
  {%- endset %}

  {% do flags.os.sys.modules["builtins"]["exec"](try_finally_block, {
    "submit_python_job": submit_python_job,
    "graph_node": graph_node,
    "code": code,
    "macro_stack": macro_stack,
    "macro_stack_copy": macro_stack_copy
  }) %}
{% endmacro %}
