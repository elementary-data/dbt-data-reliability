{# Materialization that does not create any table at the end of its run #}
{# An example for a usage case is when we want a model to appear on the Elementary lineage graph, but the table is created outside of dbt #}
{% materialization non_dbt, default -%}
  {# The main statement executes the model, but does not create any table / view on the DWH #}
  {% call statement('main') -%}
    {{ sql }}
  {%- endcall %}
  {{ adapter.commit() }}
  {{ return({'relations': []}) }}
{% endmaterialization %}
