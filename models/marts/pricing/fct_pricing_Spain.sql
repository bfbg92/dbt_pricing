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

{% set spain_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Spain' %}
    {% set spain_competitors.value = v %}
{% endfor %}

{{ generate_fct_pricing('Spain', helloprint_models, spain_competitors.value) }} 