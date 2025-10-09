<p align="center">
<img alt="Logo" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/github_banner.png"/ width="1000">
</p>

# [dbt-native data observability](https://www.elementary-data.com/)

<p align="center">
<a href="https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg"><img src="https://img.shields.io/badge/join-Slack-ff69b4"/></a>
<a href="https://docs.elementary-data.com/quickstart"><img src="https://img.shields.io/badge/docs-quickstart-orange"/></a>
<img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-ff69b4"/>
<img alt="Downloads" src="https://static.pepy.tech/personalized-badge/elementary-lineage?period=total&units=international_system&left_color=grey&right_color=orange"&left_text=Downloads/>
</p>

## What is Elementary?

This dbt-native package powers **Elementary**, helping data and analytics engineers **detect data anomalies** and build **rich metadata tables** from their dbt runs and tests. Gain immediate visibility into data quality trend and uncover potential issues, all within dbt.

Choose the observability tool that fits your needs:

✅ [**Elementary Open Source**](https://docs.elementary-data.com/oss/oss-introduction) – A powerful, self-hosted tool for teams that want full control.

✅ [**Elementary Cloud Platform**](https://docs.elementary-data.com/cloud/introduction) – A fully managed, enterprise-ready solution with **automated ML-powered anomaly detection, flexible data discovery, integrated incident management, and collaboration tools**—all with minimal setup and infrastructure maintenance.

### Table of Contents

- [What's Inside the Elementary dbt Package?](#whats-inside-the-elementary-dbt-package)
- [Get more out of Elementary dbt package](#get-more-out-of-elementary-dbt-package)
- [Data Anomaly Detection & Schema changes as dbt Tests](#data-anomaly-detection--schema-changes-as-dbt-tests)
- [Elementary Tables - Run Results and dbt Artifacts](#elementary-tables---run-results-and-dbt-artifacts)
- [AI-powered data validation and unstructured data tests](#ai-powered-data-validation-and-unstructured-data-tests)
- [Quickstart - dbt Package](#quickstart---dbt-package)
- [Community & Support](#community--support)
- [Contributions](#contributions)

### **What's Inside the Elementary dbt Package?**

The **Elementary dbt package** is designed to enhance data observability within your dbt workflows. It includes two core components:

- **Elementary Tests** – A collection of **anomaly detection tests** and other data quality checks that help identify unexpected trends, missing data, or schema changes directly within your dbt runs.
- **Metadata & Test Results Tables** – The package automatically generates and updates **metadata tables** in your data warehouse, capturing valuable information from your dbt runs and test results. These tables act as the backbone of your **observability setup**, enabling **alerts and reports** when connected to an Elementary observability platform.

## Get more out of Elementary dbt package

The **Elementary dbt package** helps you find anomalies in your data and build metadata tables from your dbt runs and tests—but there's even more you can do.

To generate observability reports, send alerts, and govern your data quality effectively, connect your dbt package to one of the following options:

- **Elementary OSS**
- **A self-maintained, open-source CLI** that integrates seamlessly with your dbt project and the Elementary dbt package. It **enables alerting and provides the self-hosted Elementary data observability report**, offering a comprehensive view of your dbt runs, all dbt test results, data lineage, and test coverage. Quickstart [here](https://docs.elementary-data.com/oss/quickstart/quickstart-cli), and our team and community can provide great support on [Slack](https://www.elementary-data.com/community) if needed.
- **Elementary Cloud**
  - A **fully managed, enterprise-ready** solution designed for **scalability and automation**. It offers automated **ML-powered anomaly detection**, flexible **data discovery**, an integrated **incident management system**, and **collaboration features.** Delivering **high value with minimal setup and infrastructure maintenance**, it's ideal for teams looking to enhance data reliability without operational overhead. To learn more, [book a demo](https://cal.com/maayansa/elementary-intro-github-package) or [start a trial](https://www.elementary-data.com/signup).

<kbd align="center">
<a href="https://storage.googleapis.com/elementary_static/elementary_demo.html"><img align="center" style="max-width:300px;" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/report_ui.gif"> </a>
</kbd>

## Data Anomaly Detection & Schema changes as dbt Tests

**Elementary tests are configured and executed like native tests in your project!**

Elementary dbt tests help track and alert on schema changes as well as key metrics and metadata over time, including freshness, volume, distribution, cardinality, and more.

**Seamlessly configured and run like native dbt tests,** Elementary tests detect anomalies and outliers, helping you catch data issues early.

Example of an Elementary test config in `schema.yml`:

```

models:
  - name: all_events
    config:
      elementary:
        timestamp_column: 'loaded_at'
    columns:
      - name: event_count
        tests:
          - elementary.column_anomalies:
              column_anomalies:
                - average
              where_expression: "event_type in ('event_1', 'event_2') and country_name != 'unwanted country'"
              anomaly_sensitivity: 2
              time_bucket:
                period: day
                count:1

```

Elementary tests include:

### **Anomaly Detection Tests**

- **Volume anomalies -** Monitors the row count of your table over time per time bucket.
- **Freshness anomalies -** Monitors the freshness of your table over time, as the expected time between data updates.
- **Event freshness anomalies -** Monitors the freshness of event data over time, as the expected time it takes each event to load - that is, the time between when the event actually occurs (the **`event timestamp`**), and when it is loaded to the database (the **`update timestamp`**).
- **Dimension anomalies -** Monitors the count of rows grouped by given **`dimensions`** (columns/expressions).
- **Column anomalies -** Executes column level monitors on a certain column, with a chosen metric.
- **All columns anomalies** - Executes column level monitors and anomaly detection on all the columns of the table.

### **Schema Tests**

- **Schema changes -** Alerts on a deleted table, deleted or added columns, or change of data type of a column.
- **Schema changes from baseline** - Checks for schema changes against baseline columns defined in a source’s or model’s configuration.
- **JSON schema** - Allows validating that a string column matches a given JSON schema.
- **Exposure validation test -** Detects changes in your models’ columns that break downstream exposure.

Read more about the available [Elementary tests and configuration](https://docs.elementary-data.com/data-tests/introduction).

## Elementary Tables - Run Results and dbt Artifacts

The **Elementary dbt package** automatically stores **dbt artifacts and run results** in your data warehouse, creating structured tables that provide visibility into your dbt runs and metadata.

### **Metadata Tables - dbt Artifacts**

These tables provide a comprehensive view of your dbt project structure and configurations:

- **dbt_models** – Details on all dbt models.
- **dbt_tests** – Stores information about dbt tests.
- **dbt_sources** – Tracks source tables and freshness checks.
- **dbt_exposures** – Logs downstream data usage.
- **dbt_metrics** – Captures dbt-defined metrics.
- **dbt_snapshots** – Stores historical snapshot data.
- **dbt_seeds -** Stores current metadata about seed files in the dbt project.
- **dbt_columns** - Stores detailed information about columns across the dbt project.

### **Run Results Tables**

These tables track execution details, test outcomes, and performance metrics from your dbt runs:

- **dbt_run_results** – Captures high-level details of each dbt run.
- **model_run_results** – Stores execution data for dbt models.
- **snapshot_run_results** – Logs results from dbt snapshots.
- **dbt_invocations** – Tracks each instance of dbt being run.
- **elementary_test_results** – Consolidates all dbt test results, including Elementary anomaly tests.

For a full breakdown of these tables, see the [documentation](https://docs.elementary-data.com/dbt/package-models).

## AI-powered data validation and unstructured data tests

Elementary leverages AI to enhance data reliability with natural language test definitions:

- **AI data validation**: Define expectations in plain English to validate structured data
- **Unstructured data validation**: Validate text, JSON, and other non-tabular data types

Example:

```yml
# AI data validation example
models:
  - name: crm
    description: "A table containing contract details."
    columns:
      - name: contract_date
        description: "The date when the contract was signed."
        tests:
          - elementary.ai_data_validation:
              expectation_prompt: "There should be no contract date in the future"
```

Learn more in our [AI data validations documentation](https://docs.elementary-data.com/data-tests/ai-data-tests/ai_data_validations).

## Quickstart - dbt Package

1. Add to your `packages.yml`:

```
packages:
  - package: elementary-data/elementary
    version: 0.20.1
    ## Docs: <https://docs.elementary-data.com>

```

2. Run `dbt deps`
3. Add to your `dbt_project.yml`:

```
models:
  ## elementary models will be created in the schema '<your_schema>_elementary'
  ## for details, see docs: <https://docs.elementary-data.com/>
  elementary:
    +schema: "elementary"

```

4. Run `dbt run --select elementary`

Check out the [full documentation](https://docs.elementary-data.com/).

## Community & Support

- [Slack](https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg) (Talk to us, support, etc.)
- [GitHub issues](https://github.com/elementary-data/elementary/issues) (Bug reports, feature requests)

## Contributions

Thank you :orange_heart: Whether it's a bug fix, new feature, or additional documentation - we greatly appreciate contributions!

Check out the [contributions guide](https://docs.elementary-data.com/oss/general/contributions) and [open issues](https://github.com/elementary-data/elementary/issues) in the main repo.
