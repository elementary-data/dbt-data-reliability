{% if execute %}
SELECT * FROM {{ var("table_name") }}
{% endif %}
