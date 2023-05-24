{% macro init_created_at() %}
    {% if not execute %}
        {% do return('') %}
    {% endif %}

    {% set created_at_exists = elementary.get_column_in_relation(this, "created_at") is not none %}
    {% if created_at_exists %}
        {% do return('') %}
    {% endif %}

    {% set add_created_at_column_query %}
        ALTER TABLE {{ this }}
        ADD COLUMN created_at {{ elementary.edr_type_timestamp() }} DEFAULT {{ elementary.edr_current_timestamp() }};
    {% endset %}
    {% do elementary.run_query(add_created_at_column_query) %}
{% endmacro %}
