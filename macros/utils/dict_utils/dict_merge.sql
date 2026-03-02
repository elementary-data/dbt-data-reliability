{#
  Merge two dicts, returning a new dict with keys from both.
  When keys overlap, `override` values take precedence over `base`.

  This is a drop-in replacement for `{% do base.update(override) %}` that
  is compatible with dbt-fusion's minijinja engine, which removed the
  `.update()` method from maps in preview.143+.

  Usage:
    {% set merged = elementary.dict_merge(base, override) %}
#}

{% macro dict_merge(base, override) %}
    {% set _result = {} %}
    {% for _k, _v in override.items() %}
        {% do _result.setdefault(_k, _v) %}
    {% endfor %}
    {% for _k, _v in base.items() %}
        {% do _result.setdefault(_k, _v) %}
    {% endfor %}
    {% do return(_result) %}
{% endmacro %}


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
