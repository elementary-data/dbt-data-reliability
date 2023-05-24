{% macro init_created_at_column() %}
    {% if not execute %}
        {% do return('') %}
    {% endif %}

    {% set created_at_exists = elementary.get_column_in_relation(this, "created_at") is not none %}
    {% if created_at_exists %}
        {% do return('') %}
    {% endif %}

    {% do elementary.create_created_at_column() %}
{% endmacro %}


{% macro create_created_at_column() %}
    {% do adapter.dispatch("create_created_at_column", "elementary")() %}
{% endmacro %}

{% macro default__create_created_at_column() %}
    {% set query %}
        ALTER TABLE {{ this }}
        ADD COLUMN created_at {{ elementary.edr_type_timestamp() }} DEFAULT {{ elementary.edr_current_timestamp() }};
    {% endset %}
    {% do elementary.run_query(query) %}
{% endmacro %}

{% macro snowflake__create_created_at_column() %}
    {% do exceptions.raise_compiler_error("Snowflake is unsupported.") %}
{% endmacro %}

{% macro bigquery__create_created_at_column() %}
    {% do elementary.add_and_alter_created_at_column() %}
{% endmacro %}

{% macro spark__create_created_at_column() %}
    {% do elementary.add_and_alter_created_at_column() %}
{% endmacro %}

{% macro add_and_alter_created_at_column() %}
    {% set add_column_query %}
        ALTER TABLE {{ this }} ADD COLUMN created_at {{ elementary.edr_type_timestamp() }};
    {% endset %}
    {% set set_default_value_query %}
        ALTER TABLE {{ this }} ALTER COLUMN created_at SET DEFAULT {{ elementary.edr_current_timestamp() }};
    {% endset %}
    {% do elementary.run_query(add_column_query) %}
    {% do elementary.run_query(set_default_value_query) %}
{% endmacro %}
