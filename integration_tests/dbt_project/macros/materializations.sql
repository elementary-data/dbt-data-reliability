{# Table #}

{% materialization table, default %}
  {% do return(elementary.materialization_table_default.call_macro()) %}
{% endmaterialization %}

{% materialization table, adapter="snowflake", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_table_snowflake.call_macro()) %}
{% endmaterialization %}

{% materialization table, adapter="bigquery", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_table_bigquery.call_macro()) %}
{% endmaterialization %}

{% materialization table, adapter="spark", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_table_spark.call_macro()) %}
{% endmaterialization %}

{% materialization table, adapter="databricks", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_table_databricks.call_macro()) %}
{% endmaterialization %}

{% materialization table, adapter="redshift", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_table_redshift.call_macro()) %}
{% endmaterialization %}

{% materialization table, adapter="athena", supported_languages=["sql"] %}
  {% do return(elementary.materialization_table_athena.call_macro()) %}
{% endmaterialization %}

{% materialization table, adapter="trino", supported_languages=["sql"] %}
  {% do return(elementary.materialization_table_trino.call_macro()) %}
{% endmaterialization %}

{# Incremental #}

{% materialization incremental, default %}
  {% do return(elementary.materialization_incremental_default.call_macro()) %}
{% endmaterialization %}

{% materialization incremental, adapter="snowflake", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_incremental_snowflake.call_macro()) %}
{% endmaterialization %}

{% materialization incremental, adapter="bigquery", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_incremental_bigquery.call_macro()) %}
{% endmaterialization %}

{% materialization incremental, adapter="spark", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_incremental_spark.call_macro()) %}
{% endmaterialization %}

{% materialization incremental, adapter="databricks", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_incremental_databricks.call_macro()) %}
{% endmaterialization %}

{% materialization incremental, adapter="athena", supported_languages=["sql"] %}
  {% do return(elementary.materialization_incremental_athena.call_macro()) %}
{% endmaterialization %}

{% materialization incremental, adapter="trino", supported_languages=["sql"] %}
  {% do return(elementary.materialization_incremental_trino.call_macro()) %}
{% endmaterialization %}

{# Test #}

{% materialization test, default %}
  {% if var('enable_elementary_test_materialization', false) %}
    {% do return(elementary.materialization_test_default.call_macro()) %}
  {% else %}
    {% do return(dbt.materialization_test_default.call_macro()) %}
  {% endif %}
{% endmaterialization %}

{% materialization test, adapter="snowflake" %}
  {% if var('enable_elementary_test_materialization', false) %}
    {% do return(elementary.materialization_test_snowflake.call_macro()) %}
  {% else %}
    {% if dbt.materialization_test_snowflake %}
      {% do return(dbt.materialization_test_snowflake.call_macro()) %}
    {% else %}
      {% do return(dbt.materialization_test_default.call_macro()) %}
    {% endif %}
  {% endif %}
{% endmaterialization %}
