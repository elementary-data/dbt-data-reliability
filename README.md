<p align="center">
<img alt="Logo" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/header_git.png"/ width="1000">
</p>
<p align="center">
Data Observability for Analytics Engineers
</p>



Elementary enables you to **monitor your data and dbt operation.** 

To learn more, refer to our [main repo](https://github.com/elementary-data/elementary), and [live demo](https://bit.ly/3IXKShW).

For reporting issues, feature requests and contributions, refer to [issues](https://github.com/elementary-data/elementary/issues) in the main repo. 


## Quick start

Add to your `packages.yml` according to your dbt version:

#### For dbt 1.2.0 and above:

```yml
packages:
  - package: elementary-data/elementary
    version: 0.4.11
    ## compatible with Elementary CLI version 0.4.11
    ## see docs: https://docs.elementary-data.com/
```

#### For dbt >=1.0.0 <1.2.0:

```yml
packages:
  - package: elementary-data/elementary
    version: 0.4.11
    ## compatible with Elementary CLI version 0.4.11
    ## see docs: https://docs.elementary-data.com/

   ## !! Important !! For dbt <1.2.0 only
   ## (Prevents dbt_utils versions exceptions) 
  - package: dbt-labs/dbt_utils
    version: [">=0.8.0", "<0.9.0"]
```

After adding to `packages.yml` and running `dbt deps`, add to your ```dbt_project.yml```:
```yml
models:

## elementary models will be created in the schema '<your_schema>_elementary'
## for details, see docs: https://docs.elementary-data.com/ 
  elementary:
    +schema: 'elementary'

```

And run ```dbt run --select elementary```.

Check out the [full documentation](https://docs.elementary-data.com/) for generating the UI, alerts and adding anomaly detection tests. 

## Run Results and dbt artifacts
The package automatically uploads the dbt artifacts and run results to your tables in your data warehouse.

Here you can find [additional details](https://docs.elementary-data.com/dbt/dbt-artifacts).


## Data anomalies detection as dbt tests 

Elementary dbt tests collect metrics and metadata over time, such as freshness, volume, schema changes, distribution, cardinality, etc. 
Executed as any other dbt tests, the Elementary tests alert on anomalies and outliers. 

**Elementary tests are configured and executed like native tests in your project!**


Example of Elementary test config in ```properties.yml```:
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
<img alt="UI" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/ui_for_git.png" width="800">

**Checkout the [live demo](https://bit.ly/3IXKShW).**



## Slack alerts
<img alt="UI" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/Slack_alert_elementary.png" width="600">



## High level architecture 
<img alt="UI" src="https://raw.githubusercontent.com/elementary-data/elementary/master/static/High_level_flow.png" width="800">




## Data warehouse support
This package has been tested on Snowflake, BigQuery and Redshift.
Additional integrations coming soon!


## Community & Support
* [Slack](https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg) (Talk to us, support, etc.)
* [GitHub issues](https://github.com/elementary-data/elementary/issues) (Bug reports, feature requests)


## Contributions

Thank you :orange_heart: Whether it’s a bug fix, new feature, or additional documentation - we greatly appreciate contributions!

Check out the [contributions guide](https://docs.elementary-data.com/general/contributions) and [open issues](https://github.com/elementary-data/elementary/issues) in the main repo. 
