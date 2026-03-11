def get_database_and_schema_properties(target: str, is_view: bool = False):
    if target == "dremio" and not is_view:
        return "datalake", "root_path"
    elif target == "clickhouse":
        return "schema", "schema"
    elif target == "spark":
        # Spark doesn't have a database property in the profile when using
        # thrift method without a catalog. Return None so callers can omit it.
        return None, "schema"
    return "database", "schema"
