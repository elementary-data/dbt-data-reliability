{# Table#}

{% materialization table, default %}
  {% do return(elementary.materialization_table_default()) %}
{% endmaterialization %}

{% materialization table, adapter="snowflake", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_table_snowflake()) %}
{% endmaterialization %}

{% materialization table, adapter="bigquery", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_table_bigquery()) %}
{% endmaterialization %}

{% materialization table, adapter="spark", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_table_spark()) %}
{% endmaterialization %}

{% materialization table, adapter="databricks", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_table_databricks()) %}
{% endmaterialization %}

{% materialization table, adapter="redshift", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_table_redshift()) %}
{% endmaterialization %}

{% materialization table, adapter="athena", supported_languages=["sql"] %}
  {% do return(elementary.materialization_table_athena()) %}
{% endmaterialization %}

{% materialization table, adapter="trino", supported_languages=["sql"] %}
  {% do return(elementary.materialization_table_trino()) %}
{% endmaterialization %}

{# Incremental #}

{% materialization incremental, default %}
  {% do return(elementary.materialization_incremental_default()) %}
{% endmaterialization %}

{% materialization incremental, adapter="snowflake", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_incremental_snowflake()) %}
{% endmaterialization %}

{% materialization incremental, adapter="bigquery", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_incremental_bigquery()) %}
{% endmaterialization %}

{% materialization incremental, adapter="spark", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_incremental_spark()) %}
{% endmaterialization %}

{% materialization incremental, adapter="databricks", supported_languages=["sql", "python"] %}
  {% do return(elementary.materialization_incremental_databricks()) %}
{% endmaterialization %}

{% materialization incremental, adapter="athena", supported_languages=["sql"] %}
  {% do return(elementary.materialization_incremental_athena()) %}
{% endmaterialization %}

{% materialization incremental, adapter="trino", supported_languages=["sql"] %}
  {% do return(elementary.materialization_incremental_trino()) %}
{% endmaterialization %}

{# Test #}

{% materialization test, default %}
  {% do return(elementary.materialization_test_default()) %}
{% endmaterialization %}

{% materialization test, adapter="snowflake" %}
  {% do return(elementary.materialization_test_snowflake()) %}
{% endmaterialization %}