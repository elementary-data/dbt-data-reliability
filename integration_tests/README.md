Tests flow:

- The schema needs to be empty


Run:
```
dbt deps
python data_generation.py
dbt seed -s training 
dbt run --full-refresh
dbt test -s tag:table_anomalies

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