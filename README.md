<p align="center">
<img alt="Logo" src="https://res.cloudinary.com/do5hrgokq/image/upload/v1764493013/github_banner_zp5l2o.png" width="1000">
</p>
<p align="center">
<a href="https://join.slack.com/t/elementary-community/shared_invite/zt-3s3uv8znb-7eBuG~ApwOa637dpVFo9Yg"><img src="https://img.shields.io/badge/join-Slack-ff69b4"/></a>
<a href="https://docs.elementary-data.com/data-tests/dbt/quickstart-package"><img src="https://img.shields.io/badge/docs-quickstart-orange"/></a>
<img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-ff69b4"/>
<img alt="Downloads" src="https://static.pepy.tech/personalized-badge/elementary-data?period=month&units=international_system&left_color=grey&right_color=orange&left_text=downloads/month" />
</p>

# [dbt-native data observability](https://www.elementary-data.com/)

From the [Elementary](https://www.elementary-data.com/) team, helping you deliver trusted data in the AI era.
Ranked among the top 5 dbt packages and supported by a growing community of thousands.

> **Need data reliability at scale?** The Elementary dbt package is also the foundation for **[Elementary Cloud](https://docs.elementary-data.com/cloud/introduction)** — a full Data & AI Control Plane with automated ML monitoring, column-level lineage from ingestion to BI and AI assets, a built-in catalog, and AI agents that scale reliability workflows for engineers and business users. [Book a demo →](https://meetings-eu1.hubspot.com/joost-boonzajer-flaes/intro-call-sl-)

---

## What it does

The package has two core components:

**1. Elementary Tables**
Using dbt's on-run-end hook, the package automatically parses your dbt artifacts and run results and loads them as structured tables into your warehouse. This includes:
- **Metadata tables** — models, tests, sources, exposures, columns, seeds, snapshots, and more
- **Run results tables** — invocations, model run results, test results, source freshness, and job-level outcomes

These tables are the backbone of any observability setup — enabling alerts, reports, and lineage when connected to Elementary OSS or Cloud. → [See full table reference](https://docs.elementary-data.com/data-tests/dbt/package-models)

**2. Elementary Tests**
A suite of anomaly detection and data quality tests that run like native dbt tests — no separate tooling. Covers volume, freshness, column distributions, schema changes, and AI-powered validation for structured and unstructured data. → [See all tests](https://docs.elementary-data.com/data-tests/introduction)

---

## Quickstart

→ [docs.elementary-data.com/data-tests/dbt/quickstart-package](https://docs.elementary-data.com/data-tests/dbt/quickstart-package)

---

## See it in action

<kbd align="center">
<a href="https://storage.googleapis.com/elementary_static/elementary_demo.html"><img align="center" style="max-width:300px;" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/report_ui.gif"> </a>
</kbd>

---

## Get the most out of the dbt package

The dbt package works standalone, and integrates with both:

- **[Elementary OSS](https://docs.elementary-data.com/oss/oss-introduction)** — Self-hosted CLI for alerts and a local observability report.
- **[Elementary Cloud](https://docs.elementary-data.com/cloud/introduction)** — A full Data & AI Control Plane with automated ML monitoring, column-level lineage from ingestion to BI and AI assets, a built-in catalog, and AI agents that scale reliability workflows for engineers and business users. [Start a trial →](https://www.elementary-data.com/signup) or [book a demo →](https://meetings-eu1.hubspot.com/joost-boonzajer-flaes/intro-call-sl-)

---

## Community & Support

- [Slack community](https://join.slack.com/t/elementary-community/shared_invite/zt-3s3uv8znb-7eBuG~ApwOa637dpVFo9Yg) — questions, support, and conversation
- [GitHub Issues](https://github.com/elementary-data/elementary/issues) — bug reports and feature requests
- [elementary-data.com](https://www.elementary-data.com/) — product, use cases, and more

Contributions are always welcome. See the [contributions guide](https://docs.elementary-data.com/oss/general/contributions) to get started. 🧡
