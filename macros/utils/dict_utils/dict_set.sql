{#
  Set a key on an existing dict in-place (overwriting if it already exists).

  Uses pop()+setdefault() for true in-place mutation — works inside for-loops
  where Jinja2 scoping prevents rebinding variables.

  WARNING: This macro uses .pop() which is NOT available in dbt-fusion's
  minijinja engine. Only use in Jinja2-only contexts (on-run-end macros,
  artifact uploads, etc.). For fusion-compatible code, use dict_update instead.

  Usage:
    {% do elementary.dict_set(my_dict, 'key', value) %}
#}

{% macro dict_set(dict, key, value) %}
    {% do dict.pop(key, none) %}
    {% do dict.setdefault(key, value) %}
{% endmacro %}
