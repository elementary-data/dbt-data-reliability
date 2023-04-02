{% macro get_json_path(json_column, json_path) -%}
    {%- set quoted_json_path = "'" ~ json_path ~ "'" %}
    {{ adapter.dispatch('get_json_path', 'elementary') (json_column, json_path, quoted_json_path) }}
{%- endmacro %}

{% macro databricks__get_json_path(json_column, json_path, quoted_json_path) %}
    get_json_object(to_json({{ json_column }}), {{ quoted_json_path }}))
{% endmacro %}

{% macro snowflake__get_json_path(json_column, json_path, quoted_json_path) %}
    json_extract_path_text(try_parse_json({{ json_column }}), {{ quoted_json_path }})
{% endmacro %}

{% macro redshift__get_json_path(json_column, json_path, quoted_json_path) %}
    case when is_valid_json({{ json_column }}) then json_extract_path_text({{ json_column }}, {{ quoted_json_path }}) else null end
{% endmacro %}

{% macro bigquery__get_json_path(json_column, json_path, quoted_json_path) %}
    json_value(to_json({{ json_column }}), {{ "'$." ~ json_path ~ "'" }} )
{% endmacro %}

{% macro postgres__get_json_path(json_column, json_path, quoted_json_path) %}
  {{ json_column }}::json->>{{ quoted_json_path }}
{% endmacro %}