{% materialization test, default %}
  {% do return(elementary.materialization_test_default.call_macro()) %}
{% endmaterialization %}

{% materialization test, adapter="snowflake" %}
  {% do return(elementary.materialization_test_snowflake.call_macro()) %}
{% endmaterialization %}
