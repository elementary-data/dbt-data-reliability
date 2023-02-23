{{ config(materialized='table') }}

SELECT
    '{{ elementary.get_elementary_package_version() }}' as dbt_pkg_version
