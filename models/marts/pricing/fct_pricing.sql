{{ config(materialized='table') }}

/* input parameters */
{% set companies = ['{{ company }}', '{{ company }}_connect', 'printoclock', 'realisaprint', 'flyeralarm'] %}


WITH 
   join_turnaround_type AS (
      SELECT 
         pm.date_price_updated,
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
         pm.date_price_updated = stt.date_price_updated AND
         pm.country_name = stt.country_name AND
         pm.product_name = stt.product_name AND
         pm.sku = stt.sku
   ),

   fill_nulls_temp as (
      SELECT 
         date_price_updated,
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
         SUM(CASE WHEN price_{{ company }} IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) as {{ company }}_partition
         {%- if not loop.last %},{% endif %}
         {% endfor -%}
      FROM join_turnaround_type
   ),

   -- At this step, null values of competitor price columns are filled with previous non-null price.
   fill_nulls AS (
      SELECT
         date_price_updated,
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
         CASE WHEN {{ company }}_partition = LAG({{ company }}_partition, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) THEN FALSE ELSE TRUE END AS price_{{ company }}_is_real,
         FIRST_VALUE(price_{{ company }}) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type, {{ company }}_partition ORDER BY date_price_updated ASC) as price_{{ company }}
         {%- if not loop.last %},{% endif %}
         {% endfor -%}
      FROM fill_nulls_temp
      ),

   -- At this step, previous competitor price columns are generated (price_lag).
   price_variation AS (
      SELECT
         date_price_updated,
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
         LAG(price_{{ company }}, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) AS price_lag_{{ company }}
         {%- if not loop.last %},{% endif %}
         {% endfor -%}
      FROM fill_nulls
      )

-- Finally, price variations are computed.
SELECT
   date_price_updated,
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
   CASE WHEN price_lag_{{ company }} IS NOT NULL THEN price_{{ company }} - price_lag_{{ company }} END as price_variation_{{ company }},
   {%- if not loop.last %},{% endif %}
   {% endfor -%}
FROM price_variation
