{% materialization dummy %}
  {% do elementary.debug_log("Dummy materialization invoked for model {}, doing nothing!".format(model.name)) %}

  {% call statement('main') -%}
    SELECT 'dummy'
  {% endcall %}

  {% do return({'relations': []}) %}
{% endmaterialization %}
