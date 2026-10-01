{% macro test_truncate_timestamp_precision(value) %}
    {{ return(elementary.truncate_timestamp_precision(value)) }}
{% endmacro %}

{% macro test_render_timestamp_casts(column_name) %}
    {{
        return(
            {
                "cast_as_timestamp": elementary.edr_cast_as_timestamp(column_name),
                "cast_metadata_timestamp": elementary.edr_cast_metadata_timestamp(
                    column_name
                ),
            }
        )
    }}
{% endmacro %}

{% macro test_cast_metadata_timestamp(value) %}
    {% set query %}
        select {{ elementary.edr_cast_metadata_timestamp(elementary.edr_quote(value)) }} as ts
    {% endset %}
    {% set ts = elementary.run_query(query).columns[0].values()[0] %}
    {{ return(ts.isoformat()) }}
{% endmacro %}
