{{ config(materialized='dummy') }}
    SELECT 1
-- depends_on: {{ ref('one') }}