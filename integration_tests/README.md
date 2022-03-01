Tests flow:

- The schema needs to be empty


Run:
```
python data_generation.py
dbt deps
dbt seed -s training 
dbt run
dbt seed -s validation
dbt run
```

Old - 
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