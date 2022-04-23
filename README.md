# Elementary - Data anomalies monitoring as dbt tests

Elementary delivers **data monitoring and anomaly detection as dbt tests.** 
  
Elementary dbt tests are data monitors that collect metrics and metadata over time, such as freshness, volume, schema changes, distribution, cardinality, etc. On each execution, the tests analyze the new data, compare it to historical metrics, and alert on anomalies and outliers. 

The package is [Elementary](https://www.elementary-data.com/) integration with your dbt project, read more here [documentation](https://docs.elementary-data.com/).
To learn more on how to monitor your DWH, refer to our [main repo](https://github.com/elementary-data/elementary-lineage). 

**Elementary data monitors as tests are configured and executed like native tests in your project!**

# dbt artifacts uploader
To enable operational monitoring and enrich anomalies alerts, the package includes a **dbt artifacts uploader**. 


## Getting started
Check out the [full documentation](https://docs.elementary-data.com/). 
Start by [using our CLI](https://github.com/elementary-data/elementary) to monitor and get Slack alerts.

## Data warehouse support
This package has been tested on Snowflake, BigQuery and Redshift.
Additional integrations coming soon!
Ask us for integrations on [Slack](https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg) or as a [GitHub issue](https://github.com/elementary-data/elementary/issues/new).


## Community, support & contributions
* [Slack](https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg) (Talk to us, support, etc.)
* [GitHub issues](https://github.com/elementary-data/elementary/issues) (Bug reports, feature requests)
