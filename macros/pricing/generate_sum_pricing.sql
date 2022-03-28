{% macro generate_sum_pricing(country, competitors) %}

{% set stg_pricing_order_items_2month_country = 'stg_bigquery-data-analytics__order_items_2month_' ~ country %}
{% set fct_pricing_country = 'fct_pricing_' ~ country %}


WITH
   dwh_data AS (
      SELECT
         sku_product_identifier,
         sku_material,
         sku_size,
         sku_printing_option,
         sku_finishing,
         total_revenue,
         order_items
      FROM {{ ref(stg_pricing_order_items_2month_country) }}
   ),

   distinct_sku_all_time_pre AS (
      SELECT
         product_name,
         material,
         size,
         COALESCE(SPLIT(sku, '-')[SAFE_OFFSET(0)],'None') as sku_product_identifier,
         COALESCE(SPLIT(sku, '-')[SAFE_OFFSET(1)],'None') as sku_material,
         COALESCE(SPLIT(sku, '-')[SAFE_OFFSET(2)],'None') as sku_size,
         COALESCE(SPLIT(sku, '-')[SAFE_OFFSET(4)],'None') as sku_printing_option,
         CONCAT('[',SPLIT(sku, '[')[SAFE_OFFSET(1)]) as sku_finishing,
         /* loop through competitors */
         {% for competitor in competitors -%}
         COUNT(DISTINCT IF (price_{{ competitor }}_is_real = TRUE, sku, NULL)) AS distinct_sku_{{ competitor }},
         {% endfor -%}
         COUNT(DISTINCT IF ((
            /* loop through competitors */
            {% for competitor in competitors -%}
            price_{{ competitor }}_is_real = TRUE
            {%- if not loop.last %} OR{% endif %}
            {% endfor -%}
         ), sku, NULL)) AS distinct_sku_all
      FROM {{ ref(fct_pricing_country) }}
      GROUP BY 1,2,3,4,5,6,7,8
   ),

   distinct_sku_all_time AS (
      SELECT
         product_name,
         material,
         size,
         /* loop through competitors */
         {% for competitor in competitors -%}
         SUM(distinct_sku_{{ competitor }}) AS distinct_sku_{{ competitor }},
         {% endfor -%}
         SUM(distinct_sku_all) AS distinct_sku_all,
         SUM(total_revenue) AS total_revenue,
         SUM(order_items) AS order_items
      FROM distinct_sku_all_time_pre dsap
         LEFT JOIN dwh_data dd ON dsap.sku_product_identifier = dd.sku_product_identifier 
            AND COALESCE(dsap.sku_material, 'None') = COALESCE(dd.sku_material, 'None')
            AND COALESCE(dsap.sku_size, 'None') = COALESCE(dd.sku_size, 'None')
            AND COALESCE(dsap.sku_printing_option, 'None') = COALESCE(dd.sku_printing_option, 'None')
            AND COALESCE(dsap.sku_finishing, 'None') = COALESCE(dd.sku_finishing, 'None')
      GROUP BY 1,2,3
   ),

   distinct_sku_2_month AS (
      SELECT
         product_name,
         material,
         size,
         /* loop through competitors */
         {% for competitor in competitors -%}
         COUNT(DISTINCT IF (price_{{ competitor }}_is_real = TRUE, sku, NULL)) AS distinct_sku_{{ competitor }},
         {% endfor -%}
         COUNT(DISTINCT IF ((
            /* loop through competitors */
            {% for competitor in competitors -%}
            price_{{ competitor }}_is_real = TRUE
            {%- if not loop.last %} OR{% endif %}
            {% endfor -%}
         ), sku, NULL)) AS distinct_sku_all
      FROM {{ ref(fct_pricing_country) }}
      WHERE spider_update_at BETWEEN DATE_SUB(CURRENT_DATE(),INTERVAL 8 WEEK) AND CURRENT_DATE()
      GROUP BY 1,2,3
   ),

   distinct_sku_4_month AS (
      SELECT
         product_name,
         material,
         size,
         /* loop through competitors */
         {% for competitor in competitors -%}
         COUNT(DISTINCT IF (price_{{ competitor }}_is_real = TRUE, sku, NULL)) AS distinct_sku_{{ competitor }},
         {% endfor -%}
         COUNT(DISTINCT IF ((
            /* loop through competitors */
            {% for competitor in competitors -%}
            price_{{ competitor }}_is_real = TRUE
            {%- if not loop.last %} OR{% endif %}
            {% endfor -%}
         ), sku, NULL)) AS distinct_sku_all
      FROM {{ ref(fct_pricing_country) }}
      WHERE spider_update_at BETWEEN DATE_SUB(CURRENT_DATE(),INTERVAL 16 WEEK) AND CURRENT_DATE()
      GROUP BY 1,2,3
   ),

   distinct_sku_6_month AS (
      SELECT
         product_name,
         material,
         size,
         /* loop through competitors */
         {% for competitor in competitors -%}
         COUNT(DISTINCT IF (price_{{ competitor }}_is_real = TRUE, sku, NULL)) AS distinct_sku_{{ competitor }},
         {% endfor -%}
         COUNT(DISTINCT IF ((
            /* loop through competitors */
            {% for competitor in competitors -%}
            price_{{ competitor }}_is_real = TRUE
            {%- if not loop.last %} OR{% endif %}
            {% endfor -%}
         ), sku, NULL)) AS distinct_sku_all
      FROM {{ ref(fct_pricing_country) }}
      WHERE spider_update_at BETWEEN DATE_SUB(CURRENT_DATE(),INTERVAL 24 WEEK) AND CURRENT_DATE()
      GROUP BY 1,2,3
   ),

   last_spider_update AS (
      SELECT
         product_name,
         MAX(spider_update_at) AS last_spider_update_at,
      FROM {{ ref(fct_pricing_country) }}
      GROUP BY 1
   ),

   distinct_sku_last_run AS (
      SELECT
         fp.product_name,
         fp.material,
         fp.size,
         lsu.last_spider_update_at,
         /* loop through competitors */
         {% for competitor in competitors -%}
         COUNT(DISTINCT IF (fp.price_{{ competitor }}_is_real = TRUE, sku, NULL)) AS last_sku_{{ competitor }},
         {% endfor -%}
         COUNT(DISTINCT IF ((
            /* loop through competitors */
            {% for competitor in competitors -%}
            fp.price_{{ competitor }}_is_real = TRUE
            {%- if not loop.last %} OR{% endif %}
            {% endfor -%}
         ), sku, NULL)) AS last_sku_all
      FROM {{ ref(fct_pricing_country) }} fp
         INNER JOIN last_spider_update lsu ON fp.spider_update_at = lsu.last_spider_update_at AND fp.product_name = lsu.product_name
      GROUP BY 1,2,3,4
      )

SELECT
   dsat.product_name,
   dsat.material,
   dsat.size,
   lsu.last_spider_update_at,
   /* loop through competitors */
   {% for competitor in competitors -%}
   COALESCE(dslr.last_sku_{{ competitor }}, 0) AS last_sku_{{ competitor }},
   {% endfor -%}
   COALESCE(dslr.last_sku_all, 0) AS last_sku_all,
   /* loop through competitors */
   {% for competitor in competitors -%}
   COALESCE(dsat.distinct_sku_{{ competitor }}, 0)  AS all_time_distinct_sku_{{ competitor }},
   {% endfor -%}
   COALESCE(dsat.distinct_sku_all, 0) AS all_time_distinct_sku_all,
   /* loop through competitors */
   {% for competitor in competitors -%}
   COALESCE(ds2m.distinct_sku_{{ competitor }}, 0) AS last_2_month_distinct_sku_{{ competitor }},
   {% endfor -%}
   COALESCE(ds2m.distinct_sku_all, 0) AS last_2_month_distinct_sku_all,
   /* loop through competitors */
   {% for competitor in competitors -%}
   COALESCE(ds4m.distinct_sku_{{ competitor }}, 0) AS last_4_month_distinct_sku_{{ competitor }},
   {% endfor -%}
   COALESCE(ds4m.distinct_sku_all, 0) AS last_4_month_distinct_sku_all,
   /* loop through competitors */
   {% for competitor in competitors -%}
   COALESCE(ds6m.distinct_sku_{{ competitor }}, 0) AS last_6_month_distinct_sku_{{ competitor }},
   {% endfor -%}
   COALESCE(ds6m.distinct_sku_all, 0) AS last_6_month_distinct_sku_all,
   COALESCE(total_revenue, 0) as revenue,
   COALESCE(order_items, 0) as order_items

FROM distinct_sku_all_time dsat
   INNER JOIN last_spider_update lsu ON dsat.product_name = lsu.product_name
   LEFT JOIN distinct_sku_last_run dslr  ON dsat.product_name = dslr.product_name AND COALESCE(dsat.material, 'None') = COALESCE(dslr.material, 'None') AND COALESCE(dsat.size, 'None') = COALESCE(dslr.size, 'None')
   LEFT JOIN distinct_sku_2_month ds2m ON dsat.product_name = ds2m.product_name AND COALESCE(dsat.material, 'None') = COALESCE(ds2m.material, 'None') AND COALESCE(dsat.size, 'None') = COALESCE(ds2m.size, 'None')
   LEFT JOIN distinct_sku_4_month ds4m ON dsat.product_name = ds4m.product_name AND COALESCE(dsat.material, 'None') = COALESCE(ds4m.material, 'None') AND COALESCE(dsat.size, 'None') = COALESCE(ds4m.size, 'None')
   LEFT JOIN distinct_sku_6_month ds6m ON dsat.product_name = ds6m.product_name AND COALESCE(dsat.material, 'None') = COALESCE(ds6m.material, 'None') AND COALESCE(dsat.size, 'None') = COALESCE(ds6m.size, 'None')

{% endmacro %}