{{
    config(
        materialized='incremental',
        partition_by={
           'field': 'spider_run_at',
           'data_type': 'date',
           'granularity': 'day'},
        unique_key='pricing_id',
        on_schema_change='fail',
        incremental_strategy='merge'
    )
}}

/* input parameters */
{% set companies = var('pricing_companies') %}
{% set competitors = var('pricing_competitors') %}


{{ generate_fct_pricing(companies,competitors) }} 