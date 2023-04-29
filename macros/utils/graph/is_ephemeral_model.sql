{% macro is_ephemeral_model(model) %}
    {% do return(
      model.identifier.startswith('__dbt__cte__')
    ) %}
{% endmacro %}
