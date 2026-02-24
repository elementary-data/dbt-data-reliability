{% test not_null_sampled(model, column_name, sample_percent=none) %}
  select * from {{ model }} where {{ column_name }} is null
{% endtest %}
