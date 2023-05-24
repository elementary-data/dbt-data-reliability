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
    {% set query %}
        ALTER TABLE {{ this }} ADD COLUMN created_at {{ elementary.edr_type_timestamp() }};
        ALTER TABLE {{ this }} ALTER COLUMN created_at SET DEFAULT {{ elementary.edr_current_timestamp() }};
    {% endset %}
    {% do elementary.run_query(query) %}
{% endmacro %}

{% macro databricks__create_created_at_column() %}
    {% set query %}
        ALTER TABLE {{ this }} ADD COLUMN created_at {{ elementary.edr_type_timestamp() }};
        ALTER TABLE {{ this }} SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
        ALTER TABLE {{ this }} ALTER COLUMN created_at SET DEFAULT {{ elementary.edr_current_timestamp() }};
    {% endset %}
    {% do elementary.run_query(query) %}
{% endmacro %}
