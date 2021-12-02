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


WITH 
   join_turnaround_type AS (
      SELECT 
         pm.spider_run_at,
         pm.country_name,
         pm.product_name,
         pm.sku,
         stt.sku_no_turnaround, -- contains Null
         pm.quantity,
         pm.turnaround,
         coalesce(stt.turnaround_type, 'between') AS turnaround_type,
         pm.material,
         pm.size,
         pm.cover,
         pm.finishing,
         cost_price,
         supplier_price,
         carrier_cost,
         /* loop through companies */
         {% for company in companies -%}
         price_{{ company }}
         {%- if not loop.last %},{% endif %}
         {% endfor -%}
      FROM {{ ref('stg_bigquery-data-analytics__pricing_monitoring') }} pm
      LEFT JOIN {{ ref('dim_sku_turnaround_type') }} stt ON 
         pm.spider_run_at = stt.spider_run_at AND
         pm.country_name = stt.country_name AND
         pm.product_name = stt.product_name AND
         pm.sku = stt.sku
      {% if is_incremental() %}
      WHERE pm.spider_run_at > (SELECT max(pm.spider_run_at) FROM {{ this }})
      {% endif %}
   ),

   fill_nulls_temp AS (
      SELECT 
         spider_run_at,
         country_name,
         product_name,
         sku,
         sku_no_turnaround,
         quantity,
         turnaround,
         turnaround_type,
         material,
         size,
         cover,
         finishing,
         cost_price,
         supplier_price,
         carrier_cost,
         /* loop through companies */
         {% for company in companies -%}
         price_{{ company }},
         SUM(CASE WHEN price_{{ company }} IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, product_name, sku ORDER BY spider_run_at ASC) AS {{ company }}_partition
         {%- if not loop.last %},{% endif %}
         {% endfor -%}
      FROM join_turnaround_type
   ),

   -- At this step, null values of competitor price columns are filled with previous non-null price.
   fill_nulls AS (
      SELECT
         spider_run_at,
         country_name,
         product_name,
         sku,
         sku_no_turnaround,
         quantity,
         turnaround,
         turnaround_type,
         material,
         size,
         cover,
         finishing,
         cost_price,
         supplier_price,
         carrier_cost,
         /* loop through companies */
         {% for company in companies -%}
         CASE WHEN {{ company }}_partition = LAG({{ company }}_partition, 1) OVER (PARTITION BY country_name, product_name, sku ORDER BY spider_run_at ASC) THEN FALSE ELSE TRUE END AS price_{{ company }}_is_real,
         FIRST_VALUE(price_{{ company }}) OVER (PARTITION BY country_name, product_name, sku, {{ company }}_partition ORDER BY spider_run_at ASC) AS price_{{ company }}
         {%- if not loop.last %},{% endif %}
         {% endfor -%}
      FROM fill_nulls_temp
      ),

   -- At this step, previous competitor price columns are generated (price_lag).
   price_variation AS (
      SELECT
         spider_run_at,
         country_name,
         product_name,
         sku,
         sku_no_turnaround,
         quantity,
         turnaround,
         turnaround_type,
         material,
         size,
         cover,
         finishing,
         cost_price,
         supplier_price,
         carrier_cost,
         /* loop through companies */
         {% for company in companies -%}
         CASE WHEN price_{{ company }} IS NULL THEN NULL ELSE price_{{ company }}_is_real END AS price_{{ company }}_is_real,
         price_{{ company }},
         LAG(price_{{ company }}, 1) OVER (PARTITION BY country_name, product_name, sku ORDER BY spider_run_at ASC) AS price_lag_{{ company }}
         {%- if not loop.last %},{% endif %}
         {% endfor -%}
      FROM fill_nulls
      )

-- Finally, the following metrics are computed per company: price_variation, GPM.
SELECT
   {{ dbt_utils.surrogate_key(['spider_run_at', 'country_name', 'product_name', 'sku']) }} as pricing_id,
   spider_run_at,
   country_name,
   product_name,
   sku,
   sku_no_turnaround,
   quantity,
   turnaround,
   turnaround_type,
   material,
   size,
   cover,
   finishing,
   cost_price,
   supplier_price,
   carrier_cost,
   /* loop through companies */
   {% for company in companies -%}
   price_{{ company }}_is_real,
   price_{{ company }},
   CASE WHEN price_lag_{{ company }} IS NOT NULL THEN price_{{ company }} - price_lag_{{ company }} END AS price_variation_{{ company }},
   (price_{{ company }} - cost_price)/price_{{ company }} AS gpm_{{ company }},
   {% endfor -%}
   /* loop through competitors */
   {% for competitor in competitors -%}
   price_helloprint - price_{{ competitor }} AS helloprint_diff_{{ competitor }},
   price_helloprint_connect - price_{{ competitor }} AS helloprint_connect_diff_{{ competitor }},
   CASE
      WHEN price_helloprint = price_{{ competitor }} THEN 'Same'
      WHEN price_helloprint > price_{{ competitor }} THEN 'Higher'
      WHEN price_helloprint < price_{{ competitor }} THEN 'Lower'      
      ELSE 'Unknown' END AS helloprint_vs_{{ competitor }},
   CASE
      WHEN price_helloprint_connect = price_{{ competitor }} THEN 'Same'
      WHEN price_helloprint_connect > price_{{ competitor }} THEN 'Higher'
      WHEN price_helloprint_connect < price_{{ competitor }} THEN 'Lower'      
      ELSE 'Unknown' END AS helloprint_connect_vs_{{ competitor }}
   {%- if not loop.last %},{% endif %}
   {% endfor -%}
FROM price_variation