{% macro type_safe_tojson(val) %}
  {% if elementary.is_primitive(val) %}
    {% do return(val) %}

  {% elif val is mapping %}
    {% for k, v in val.items() %}
      {% do val.update({k: elementary.type_safe_tojson(v)}) %}
    {% endfor %}
    {% do return(val) %}

  {% elif val is iterable %}
    {% set new_list = [] %}
    {% for item in val %}
      {% do new_list.append(elementary.type_safe_tojson(item)) %}
    {% endfor %}
    {% do return(new_list) %}

  {% else %}
    {% do return(val | string) %}
  {% endif %}
{% endmacro %}
