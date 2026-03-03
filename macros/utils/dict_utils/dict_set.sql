{#
  Set a key on an existing dict in-place (overwriting if it already exists).

  This is a fusion-compatible replacement for `{% do dict.update({key: value}) %}`
  that works inside for loops and other contexts where Jinja2 scoping prevents
  rebinding variables.

  Usage:
    {% do elementary.dict_set(my_dict, 'key', value) %}
#}

{% macro dict_set(dict, key, value) %}
    {% do dict.pop(key, none) %}
    {% do dict.setdefault(key, value) %}
{% endmacro %}
