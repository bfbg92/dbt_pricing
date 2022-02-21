{% macro generate_fct_pricing(country, helloprint_models, competitors) %}

{% set stg_pricing_monitoring_country = 'stg_bigquery-data-analytics__pricing_monitoring_' ~ country %}
{% set dim_sku_turnaround_type_country = 'dim_sku_turnaround_type_' ~ country %}

WITH 
   join_turnaround_type AS (
      SELECT 
         pm.spider_update_at,
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

         /* loop through helloprint_models */
         {% for model in helloprint_models -%}
         price_{{ model }},
         {% endfor -%}
         /* loop through competitors */
         {% for competitor in competitors -%}
         price_{{ competitor }}
         {%- if not loop.last %},{% endif %}
         {% endfor -%}

      FROM {{ ref(stg_pricing_monitoring_country) }} pm
      LEFT JOIN {{ ref(dim_sku_turnaround_type_country) }} stt ON 
         pm.spider_update_at = stt.spider_update_at AND
         pm.product_name = stt.product_name AND
         pm.sku = stt.sku

      {% if is_incremental() %}
      AND pm.spider_update_at > (SELECT max(pm.spider_update_at) FROM {{ this }})
      {% endif %}
   ),

   fill_nulls_temp AS (
      SELECT 
         spider_update_at,
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

         /* loop through helloprint_models */
         {% for model in helloprint_models -%}
         price_{{ model }},
         SUM(CASE WHEN price_{{ model }} IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY product_name, sku ORDER BY spider_update_at ASC) AS {{ model }}_partition,
         {% endfor -%}

         /* loop through competitors */
         {% for competitor in competitors -%}
                  price_{{ competitor }},
         SUM(CASE WHEN price_{{ competitor }} IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY product_name, sku ORDER BY spider_update_at ASC) AS {{ competitor }}_partition
         {%- if not loop.last %},{% endif %}
         {% endfor -%}
      FROM join_turnaround_type
   ),

   -- At this step, null values of competitor price columns are filled with previous non-null price.
   fill_nulls AS (
      SELECT
         spider_update_at,
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

         /* loop through helloprint_models */
         {% for model in helloprint_models -%}
         CASE WHEN {{ model }}_partition = LAG({{ model }}_partition, 1) OVER (PARTITION BY product_name, sku ORDER BY spider_update_at ASC) THEN FALSE ELSE TRUE END AS price_{{ model }}_is_real,
         FIRST_VALUE(price_{{ compemodeltitor }}) OVER (PARTITION BY product_name, sku, {{ model }}_partition ORDER BY spider_update_at ASC) AS price_{{ model }},
         {% endfor -%}

         /* loop through competitors */
         {% for competitor in competitors -%}
         CASE WHEN {{ competitor }}_partition = LAG({{ competitor }}_partition, 1) OVER (PARTITION BY product_name, sku ORDER BY spider_update_at ASC) THEN FALSE ELSE TRUE END AS price_{{ competitor }}_is_real,
         FIRST_VALUE(price_{{ competitor }}) OVER (PARTITION BY product_name, sku, {{ competitor }}_partition ORDER BY spider_update_at ASC) AS price_{{ competitor }}
         {%- if not loop.last %},{% endif %}
         {% endfor -%}

      FROM fill_nulls_temp
      ),

   -- At this step, previous competitor price columns are generated (price_lag).
   price_variation AS (
      SELECT
         spider_update_at,
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

         /* loop through helloprint_models */
         {% for model in helloprint_models -%}
         CASE WHEN price_{{ model }} IS NULL THEN NULL ELSE price_{{ model }}_is_real END AS price_{{ model }}_is_real,
         price_{{ model }},
         LAG(price_{{ model }}, 1) OVER (PARTITION BY product_name, sku ORDER BY spider_update_at ASC) AS price_lag_{{ model }},
         {% endfor -%}

         /* loop through competitors */
         {% for competitor in competitors -%}
         CASE WHEN price_{{ competitor }} IS NULL THEN NULL ELSE price_{{ competitor }}_is_real END AS price_{{ competitor }}_is_real,
         price_{{ competitor }},
         LAG(price_{{ competitor }}, 1) OVER (PARTITION BY product_name, sku ORDER BY spider_update_at ASC) AS price_lag_{{ competitor }}
         {%- if not loop.last %},{% endif %}
         {% endfor -%}
      FROM fill_nulls
      )

-- Finally, the following metrics are computed per helloprint_model/competitor: price_variation, GPM.
SELECT
   {{ dbt_utils.surrogate_key(['spider_update_at', 'product_name', 'sku']) }} as pricing_id,
   spider_update_at,
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

   /* loop through helloprint_models */
   {% for model in helloprint_models -%}
   price_{{ model }}_is_real,
   price_{{ model }},
   CASE WHEN price_lag_{{ model }} IS NOT NULL THEN price_{{ model }} - price_lag_{{ model }} END AS price_variation_{{ model }},
   (price_{{ model }} - cost_price)/price_{{ model }} AS gpm_{{ model }},
   {% endfor -%}

   /* loop through competitors */
   {% for competitor in competitors -%}
   price_{{ competitor }}_is_real,
   price_{{ competitor }},
   CASE WHEN price_lag_{{ competitor }} IS NOT NULL THEN price_{{ competitor }} - price_lag_{{ competitor }} END AS price_variation_{{ competitor }},
   (price_{{ competitor }} - cost_price)/price_{{ competitor }} AS gpm_{{ competitor }},
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


{% endmacro %}