{{ config(materialized='dummy') }}
    begin;
    commit;
-- depends_on: {{ ref('one') }}