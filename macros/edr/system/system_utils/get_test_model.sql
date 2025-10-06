{% macro get_test_model() %}
  {# This macro is used to get the global "model" object for tests,
     we created it since context["model"] doesn't return the same thing in dbt-fusion #}

  {% do return(model) %}
{% endmacro %}
