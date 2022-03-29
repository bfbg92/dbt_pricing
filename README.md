# DWH DBT
Release 1.0.0
<br><br>

## About
At Helloprint DBT (Data Build Tool) is used to enable analytic-engineering. The tool is used to transform the data that lives on our warehouse by simply writing select statements. DBT handles turning these select statements into tables and views.<br><br>

## Installation
#### Use Homebrew to install dbt
```
$ brew update
$ brew install git
$ brew tap dbt-labs/dbt
```

#### Use Homebrew to install a bigquery dbt adapter
```
$ brew install dbt-bigquery
```

#### Check if dbt was installed successfully
```
$ dbt --version
```

#### Setup profiles.yml
In this file a profile is configurated that tells dbt how to operate on your dwh (e.g. bigquery)
```
dwh_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /Users/bruno.guerreiro/.dbt/dbt-user-creds.json
      project: helloprint-data-analytics-live
      dataset: dbt_bg
      threads: 1
      timeout_seconds: 300
      location: EU
      priority: interactive
```
It goes under: `~/.dbt/dbt-user-creds.json`.

#### Generate BigQuery credentials
Ask your responsible for the bigquery service account keys
It goes under: `~/.dbt/profiles.yml`.
<br><br>

## Project Structure
```
├── dbt_project.yml
├── packages.yml
└── models
    ├── marts
    |   ├── core
    |   ├── finance
    |   ├── marketing
    |   ├── . . .
    │   └── pricing
    │       ├── pricing.yml
    │       ├── dim_sku_turnaround_type_Belgium.sql
    │       ├── . . .
    │       ├── dim_sku_turnaround_type_United_Kingdom.sql
    │       ├── fct_pricing_Belgium.sql
    │       ├── . . .
    │       └── fct_pricing_United_Kingdom.sql
    └── staging
        ├── presta
        ├── gcloud
        ├── . . .
        └── bigquery-data-analytics
            ├── src_bigquery-data-analytics.yml
            ├── stg_bigquery-data-analytics.yml
            ├── stg_bigquery-data-analytics__pricing_monitoring_Belgium.sql
            ├── . . .
            └── stg_bigquery-data-analytics__pricing_monitoring_United_Kingdom.sql
```

1. Sources: Schemas and tables in a source-conformed structure (i.e. tables and columns in a structure based on what an API returns), loaded by a third party tool. <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; They go under: `models/staging/<data_source>/src_<data_source>.yml`. <br>
<br>

2. Staging models: The atomic unit of data modeling. Each model bears a one-to-one relationship with the source data table it represents. It has the same granularity, but the columns have been renamed, recast, or usefully reconsidered into a consistent format. <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; They go under: `models/staging/<data_source>/` <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; The name convention for the models is: `stg_<data_source>_<model_name>.sql` <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; The intermediate staging models go under: `models/staging/<data_source>/intermediate` <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; The name convention for the intermediate staging models is: `sint_<data_source>_<model_name>.sql` <br>
<br>

3. Marts models: Models that represent business processes and entities, abstracted from the data sources that they are based on. <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; They go under: `models/marts/<business_area>/` <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; The name convention for the models is: `dim_<business_area>_<model_name>.sql` or `fct_<business_area>_<model_name>.sql` <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; The intermediate marts models go under: `models/marts/<business_area>/intermediate` <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; The name convention for the intermediate marts models is: `mint_<business_area>_<model_name>.sql` <br>
<br><br>

## Useful Commands

#### General
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt run` - regular run <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt run --full-refresh` - will refresh incremental models <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt clean` - this will remove the /dbt_modules (populated when you run deps) and /target folder (populated when models are run) <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt test` - will run custom data tests and schema tests <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt seed` - will load csv files specified in the data-paths directory into the data warehouse. <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt compile` - compiles all models. <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `make dbt-docs` - spins up a local container to serve you the dbt docs in a web-browser (localhost:8081) <br>
<br>
#### Selective by name
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt run --models modelname` - will only run modelname <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt run --models +modelname` - will run modelname and all parents <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt run --models modelname+` - will run modelname and all children <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt run --models +modelname+` - will run modelname, and all parents and children <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt run --models @modelname` - will run modelname, all parents, all children, AND all parents of all children <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt run --exclude modelname` - will run all models except modelname <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; *also works for `dbt test`
<br>
####  Selective by folder
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt run --models folder` - will run all models in a folder <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt run --models folder.subfolder` - will run all models in the subfolder <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; `dbt run --models +folder.subfolder` - will run all models in the subfolder and all parents <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; *also works for `dbt test`
