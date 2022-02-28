{{
  config(
    materialized='table',
  )
}}

{{ monitors_query('1,2,3,4') }}
