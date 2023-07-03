{{ config(materialized='view') }}
    begin;
    commit;
-- depends_on: {{ ref('one') }}