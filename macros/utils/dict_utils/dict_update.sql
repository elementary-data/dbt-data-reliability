{#
  Merge source keys into target, returning a NEW dict.
  Source values take precedence over target values when keys overlap.

  This is a fusion-compatible replacement for `{% do target.update(source) %}`
  that works with dbt-fusion's minijinja engine (preview.143+).

  NOTE: Returns a new dict — callers must rebind the variable:
    {% set my_dict = elementary.dict_update(my_dict, {"key1": val1}) %}

  For true in-place single-key mutation in Jinja2-only contexts
  (e.g. inside for-loops during on-run-end), use dict_set instead.
#}

{% macro dict_update(target, source) %}
    {% set _result = {} %}
    {% for _k, _v in source.items() %}
        {% do _result.setdefault(_k, _v) %}
    {% endfor %}
    {% for _k, _v in target.items() %}
        {% do _result.setdefault(_k, _v) %}
    {% endfor %}
    {% do return(_result) %}
{% endmacro %}
