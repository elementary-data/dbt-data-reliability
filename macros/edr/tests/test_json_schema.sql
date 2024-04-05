{% test json_schema(model, column_name, where_expression) %}
    {{ config(fail_calc = 'fail_count', tags=['elementary-tests']) }}

    {% if not execute or not elementary.is_test_command() or not elementary.is_elementary_enabled() %}
        {% do return(none) %}
    {% endif %}

    {% if model is string %}
        {{ exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you provide a 'where' parameter to the test or override 'ref' or 'source')") }}
    {% endif %}

    {% if not elementary.column_exists_in_relation(model, column_name) %}
        {% do exceptions.raise_compiler_error("Column '{}' does not exist in node '{}'!".format(column_name, model.name)) %}
    {% endif %}
    {% if not kwargs %}
        {% do exceptions.raise_compiler_error("A json schema must be supplied as a part of the test!") %}
    {% endif %}

    {{ elementary.test_python(model, elementary.json_schema_python_test, {'column_name': column_name, 'json_schema': kwargs}, where_expression,
                              packages=['jsonschema']) }}
{% endtest %}

{% macro json_schema_python_test(args) %}
import json
import jsonschema

def is_valid_json(val, json_schema):
    try:
        jsonschema.validate(json.loads(val), json_schema)
        return True
    except (json.JSONDecodeError, jsonschema.ValidationError):
        return False

def get_column_name_in_df(df, column_name):
    matching = [col for col in df.columns if col.lower() == column_name.lower()]
    if len(matching) > 1:
        # Case matters, switch to case-sensitive match
        matching = [col for col in df.columns if col == column_name]

    if len(matching) == 0:
        raise Exception("No column with the name: {}".format(col))
    return matching[0]

def test(model_df, ref, session):
    raw_json_schema = r"""{{ args.json_schema if args.json_schema is string else tojson(args.json_schema) }}"""
    json_schema = json.loads(raw_json_schema)

    model_df = model_df.toPandas()
    column_name = get_column_name_in_df(model_df, "{{ args.column_name }}")
    model_df["is_valid_json"] = model_df[column_name].apply(lambda val: is_valid_json(val, json_schema))

    return model_df[model_df.is_valid_json == False]
{% endmacro %}
