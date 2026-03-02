{% macro is_ephemeral_model(model_relation) %}
    {% do return(
      model_relation.is_cte
    ) %}
{% endmacro %}
