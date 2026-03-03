{#
  Update an existing dict in-place with keys from another dict.

  This is a fusion-compatible replacement for `{% do target.update(source) %}`
  that works with dbt-fusion's minijinja engine (preview.143+).

  Usage:
    {% do elementary.dict_update(my_dict, {"key1": val1, "key2": val2}) %}
#}

{% macro dict_update(target, source) %}
    {% for _k, _v in source.items() %}
        {% do elementary.dict_set(target, _k, _v) %}
    {% endfor %}
{% endmacro %}
