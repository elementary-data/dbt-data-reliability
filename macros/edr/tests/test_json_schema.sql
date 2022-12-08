{% test test_json_schema(model, column, json_schema) %}
    {{ elementary.test_python(model, elementary.json_schema_python_test, {'column': column, 'json_schema': json_schema},
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

def get_column_name_in_df(df, column):
    matching = [col for col in df.columns if col.lower() == column.lower()]
    if len(matching) == 0:
        raise Exception("No column with the name: {}".format(col))
    elif len(matching) > 1:
        # If there's more than one match, then case matters, so return as-is
        return column

    return matching[0]


def test(model_df, ref, session):
    raw_json_schema = r"""{{ args.json_schema if args.json_schema is string else tojson(args.json_schema) }}"""
    json_schema = json.loads(raw_json_schema)

    model_df = model_df.toPandas()
    column_name = get_column_name_in_df(model_df, "{{ args.column }}")
    model_df["is_valid_json"] = model_df[column_name].apply(lambda val: is_valid_json(val, json_schema))

    return model_df[model_df.is_valid_json == False]
{% endmacro %}