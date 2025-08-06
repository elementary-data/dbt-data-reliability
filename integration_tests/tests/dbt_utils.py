def get_database_and_schema_properties(target: str, is_view: bool = False):
    if target == "dremio" and not is_view:
        return "datalake", "root_path"
    elif target == "clickhouse":
        return "schema", "schema"
    return "database", "schema"
