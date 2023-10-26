<p align="center">
<img alt="Logo" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/github_banner.png"/ width="1000">
</p>

<h2 align="center">
 dbt native data observability for analytics & data engineers
</h2>
<h4 align="center">
Monitor your data quality, operation and performance directly from your dbt project.
</h4>

<p align="center">
<a href="https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg"><img src="https://img.shields.io/badge/join-Slack-ff69b4"/></a>
<a href="https://docs.elementary-data.com/quickstart"><img src="https://img.shields.io/badge/docs-quickstart-orange"/></a>
<img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-ff69b4"/>
<img alt="Downloads" src="https://static.pepy.tech/personalized-badge/elementary-lineage?period=total&units=international_system&left_color=grey&right_color=orange"&left_text=Downloads"/>

## Quick start

1. Add to your `packages.yml`:

```yml packages.yml
packages:
  - package: elementary-data/elementary
    version: 0.12.0
    ## Docs: https://docs.elementary-data.com
```

2. Run `dbt deps`

3. Add to your `dbt_project.yml`:

```yml
models:
  ## elementary models will be created in the schema '<your_schema>_elementary'
  ## for details, see docs: https://docs.elementary-data.com/
  elementary:
    +schema: "elementary"
```

4. Run `dbt run --select elementary`

Check out the [full documentation](https://docs.elementary-data.com/) for generating the UI, alerts and adding anomaly detection tests.

## Run Results and dbt artifacts

The package automatically uploads the dbt artifacts and run results to tables in your data warehouse:

Run results tables:

- dbt_run_results
- model_run_results
- snapshot_run_results
- dbt_invocations
- elementary_test_results (all dbt test results)

Metadata tables:

- dbt_models
- dbt_tests
- dbt_sources
- dbt_exposures
- dbt_metrics
- dbt_snapshots

Here you can find [additional details about the tables](https://docs.elementary-data.com/guides/modules-overview/dbt-package).

## Data anomalies detection as dbt tests

Elementary dbt tests collect metrics and metadata over time, such as freshness, volume, schema changes, distribution, cardinality, etc.
Executed as any other dbt tests, the Elementary tests alert on anomalies and outliers.

**Elementary tests are configured and executed like native tests in your project!**

Example of Elementary test config in `properties.yml`:

```yml
models:
  - name: your_model_name
    config:
      elementary:
        timestamp_column: updated_at
    tests:
      - elementary.table_anomalies
      - elementary.all_columns_anomalies
```

## Data observability report

<kbd align="center">
        <a href="https://storage.googleapis.com/elementary_static/elementary_demo.html"><img align="center" style="max-width:300px;" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/report_ui.gif"> </a>
</kbd>

## Slack alerts

<img alt="UI" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/Slack_alert_elementary.png" width="600">

## How it works?

Elementary dbt package creates tables of metadata and test results in your data warehouse, as part of your dbt runs. The [CLI tool](https://github.com/elementary-data/elementary) reads the data from these tables, and is used to generate the UI and alerts.

<img align="center" style="max-width:300px;" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/how_elementary_works.png">

## Data warehouse support

- [x] **Snowflake** ![](https://raw.githubusercontent.com/elementary-data/elementary/master/static/snowflake-16.png)
- [x] **BigQuery** ![](https://raw.githubusercontent.com/elementary-data/elementary/master/static/bigquery-16.svg)
- [x] **Redshift** ![](https://raw.githubusercontent.com/elementary-data/elementary/master/static/redshift-16.png)
- [x] **Databricks SQL** ![](https://raw.githubusercontent.com/elementary-data/elementary/master/static/databricks-16.png)
- [x] **Postgres** ![](https://raw.githubusercontent.com/elementary-data/elementary/master/static/postgres-16.png)

## Community & Support

- [Slack](https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg) (Talk to us, support, etc.)
- [GitHub issues](https://github.com/elementary-data/elementary/issues) (Bug reports, feature requests)

## Contributions

Thank you :orange_heart: Whether it’s a bug fix, new feature, or additional documentation - we greatly appreciate contributions!

Check out the [contributions guide](https://docs.elementary-data.com/general/contributions) and [open issues](https://github.com/elementary-data/elementary/issues) in the main repo.
