{% macro init_insertion_time_default() %}
ALTER TABLE {{ this }} ALTER COLUMN insertion_time SET DEFAULT NOW();
{% endmacro %}
