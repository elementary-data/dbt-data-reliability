Tests flow:

- The schema needs to be empty


Run:
```
dbt seed
dbt snapshot
dbt run
dbt run-operation do_schema_changes
dbt snapshot
dbt run
dbt run-operation do_configuration_changes
dbt snapshot
dbt run
dbt test --store-failures
```