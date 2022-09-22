{% set target_relation = api.Relation.create(database=elementary.target_database(), schema=target.schema, identifier='numeric_column_anomalies') %}

select min from {{ target_relation }} where min < 100
