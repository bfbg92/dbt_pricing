{{
    config(
        materialized='incremental',
        partition_by={
           'field': 'spider_update_at',
           'data_type': 'date',
           'granularity': 'day'},
        unique_key='pricing_id',
        on_schema_change='fail',
        incremental_strategy='merge'
    )
}}

/* input parameters */
{% set helloprint_models = var('pricing_helloprint_models') %}
{% set competitors = var('pricing_competitors') %}

{% set germany_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Germany' %}
    {% set germany_competitors.value = v %}
{% endfor %}

{{ generate_fct_pricing('Germany', helloprint_models, germany_competitors.value) }}