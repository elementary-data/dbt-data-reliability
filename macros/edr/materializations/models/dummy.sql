{% materialization dummy %}
  {% do elementary.debug_log("Dummy materialization invoked for model {}, doing nothing!".format(model.name)) %}

  {% set identifier = model['alias'] %}
  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema, database=database,
      type='table') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main') -%}
    SELECT 'dummy'
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do return({'relations': [target_relation]}) %}
{% endmaterialization %}
