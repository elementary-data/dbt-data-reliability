<p align="center">
<img alt="Logo" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/github_banner.png"/ width="1000">
</p>

# [dbt-native data observability](https://www.elementary-data.com/)

<p align="center">
<a href="https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg"><img src="https://img.shields.io/badge/join-Slack-ff69b4"/></a>
<a href="https://docs.elementary-data.com/quickstart"><img src="https://img.shields.io/badge/docs-quickstart-orange"/></a>
<img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-ff69b4"/>
<img alt="Downloads" src="[https://static.pepy.tech/personalized-badge/elementary-lineage?period=total&units=international_system&left_color=grey&right_color=orange"&left_text=Downloads"/](https://static.pepy.tech/personalized-badge/elementary-lineage?period=total&units=international_system&left_color=grey&right_color=orange%22&left_text=Downloads%22/)>
</p>

## What is Elementary?

This dbt-native package powers **Elementary**, helping data and analytics engineers **test for data anomalies** and build **rich metadata tables** from their dbt runs and tests. Gain immediate visibility into data quality trends, **detect anomalies**, and uncover potential issues—all within dbt.

Choose the observability tool that fits your needs:

✅ [**Elementary Open Source**](https://docs.elementary-data.com/oss/oss-introduction) – A powerful, self-hosted tool for teams that want full control.

✅ [**Elementary Cloud Platform**](https://docs.elementary-data.com/cloud/introduction) – A fully managed, enterprise-ready solution with **ML-powered anomaly detection, flexible data discovery, integrated incident management, and collaboration tools**—all with minimal setup and infrastructure maintenance.

### Table of Contents

- What's Inside the Elementary dbt Package?
- Quick start - dbt package
- Get more out of Elementary dbt package
- Metadata tables - Run Results and dbt artifacts
- Data anomaly detection as dbt tests
- [Community & Support](https://www.notion.so/dbt-README-1ab621a084bf8027ac32e8b6f663a231?pvs=21)
- [Contribution](https://www.notion.so/dbt-README-1ab621a084bf8027ac32e8b6f663a231?pvs=21)

### **What's Inside the Elementary dbt Package?**

The **Elementary dbt package** is designed to enhance data observability within your dbt workflows. It includes two core components:

- **Elementary Tests** – A collection of **anomaly detection tests** and other data quality checks that help identify unexpected trends, missing data, or schema changes directly within your dbt runs.
- **Metadata & Test Results Tables** – The package automatically generates and updates **metadata tables** in your data warehouse, capturing valuable information from your dbt runs and test results. These tables act as the backbone of your **observability setup**, enabling **alerts and reports** when connected to an Elementary observability platform.

## Quick start - dbt package

1. Add to your `packages.yml`:

```
packages:
  - package: elementary-data/elementary
    version: 0.17.0
    ## Docs: <https://docs.elementary-data.com>

```

1. Run `dbt deps`
2. Add to your `dbt_project.yml`:

```
models:
  ## elementary models will be created in the schema '<your_schema>_elementary'
  ## for details, see docs: <https://docs.elementary-data.com/>
  elementary:
    +schema: "elementary"

```

1. Run `dbt run --select elementary`

Check out the [full documentation](https://docs.elementary-data.com/).

## Get more out of Elementary dbt package

The **Elementary dbt package** helps you find anomalies in your data and build metadata tables from your dbt runs and tests—but there's even more you can do.

To generate observability reports, send alerts, and govern your data quality effectively, connect your dbt package to one of the following options:

- **Elementary OSS**
    - An open-source CLI tool you can **deploy and orchestrate to send alerts** and **self-host the Elementary report**. Best for data and analytics engineers that require basic observability capabilities or for evaluating features without vendor approval. Quickstart [here](https://docs.elementary-data.com/oss/quickstart/quickstart-cli), and our team and community can provide great support on [Slack](https://www.elementary-data.com/community) if needed.
- **Elementary Cloud**
    - A **fully managed, enterprise-ready** solution designed for **scalability and automation**. It offers **ML-powered anomaly detection**, flexible **data discovery**, an integrated **incident management system**, and **collaboration features.** Delivering **high value with minimal setup and infrastructure maintenance**, it's ideal for teams looking to enhance data reliability without operational overhead. To learn more, [book a demo](https://cal.com/maayansa/elementary-intro-github-package) or [start a trial](https://www.elementary-data.com/signup).

<kbd align="center">
<a href="https://storage.googleapis.com/elementary_static/elementary_demo.html"><img align="center" style="max-width:300px;" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/report_ui.gif"> </a>
</kbd>

## Elementary tables - Run Results and dbt artifacts

The **Elementary dbt package** automatically stores **dbt artifacts and run results** in your data warehouse, creating structured tables that provide visibility into your dbt runs and metadata.

### **Run Results Tables**

These tables track execution details, test outcomes, and performance metrics from your dbt runs:

- **dbt_run_results** – Captures high-level details of each dbt run.
- **model_run_results** – Stores execution data for dbt models.
- **snapshot_run_results** – Logs results from dbt snapshots.
- **dbt_invocations** – Tracks each instance of dbt being run.
- **elementary_test_results** – Consolidates all dbt test results, including Elementary anomaly tests.

### **Metadata Tables**

These tables provide a comprehensive view of your dbt project structure and configurations:

- **dbt_models** – Details on all dbt models.
- **dbt_tests** – Stores information about dbt tests.
- **dbt_sources** – Tracks source tables and freshness checks.
- **dbt_exposures** – Logs downstream data usage.
- **dbt_metrics** – Captures dbt-defined metrics.
- **dbt_snapshots** – Stores historical snapshot data.

For a full breakdown of these tables, see the [documentation](https://docs.elementary-data.com/guides/modules-overview/dbt-package).

## Data anomaly detection as dbt tests

**Elementary tests are configured and executed like native tests in your project!**

Elementary dbt tests track key metrics and metadata over time, including freshness, volume, schema changes, distribution, cardinality, and more. 

**Seamlessly configured and run like native dbt tests,** Elementary tests detect anomalies and outliers, helping you catch data issues early.

Example of Elementary test config in `properties.yml`:

```
models:
  - name: your_model_name
    config:
      elementary:
        timestamp_column: updated_at
    tests:
      - elementary.table_anomalies
      - elementary.all_columns_anomalies

```

Read more about the available [Elementary tests and configuration](https://docs.elementary-data.com/data-tests/introduction).

## Community & Support

- [Slack](https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg) (Talk to us, support, etc.)
- [GitHub issues](https://github.com/elementary-data/elementary/issues) (Bug reports, feature requests)

## Contributions

Thank you :orange_heart: Whether it’s a bug fix, new feature, or additional documentation - we greatly appreciate contributions!

Check out the [contributions guide](https://docs.elementary-data.com/general/contributions) and [open issues](https://github.com/elementary-data/elementary/issues) in the main repo.
