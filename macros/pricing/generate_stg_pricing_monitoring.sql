{% macro generate_stg_pricing_monitoring(country, competitors, competitors_mapped, products) %}


WITH 

map_filter_rename AS (
  SELECT
    CAST(time_price_updated AS DATE) AS spider_update_at,
    LOWER(product_name) AS product_name,
    sku,
    quantity,
    turnaround,
    material,
    size,
    cover,
    finishing,
    {{ map_comp('competitor_name') }} AS competitor_renamed,
    competitor_price_comp_r1,
    salesprice_comp_r1 AS price_helloprint,
    salesprice_comp_all AS price_helloprint_connect,
    cost_price,
    supplier_price,
    carrier_cost
  FROM {{ source('bigquery-data-analytics', 'pricing_monitoring') }}
  WHERE
    country_name = '{{ country }}' AND
    product_name IN ('{{ "','".join(products) }}') AND
    competitor_name IN ('{{ "','".join(competitors) }}') AND (
      (salesprice_comp_r1 IS NOT NULL AND salesprice_comp_r1 > 0) OR
      (salesprice_comp_all IS NOT NULL AND salesprice_comp_all > 0))),

grouped AS (
  SELECT
    spider_update_at,
    product_name,
    sku,
    quantity,
    turnaround,
    material,
    size,
    cover,
    finishing,
    /* loop through competitors */
    {% for competitor in competitors_mapped -%}
    MAX(
      if(
        competitor_renamed = '{{ competitor }}'
        AND competitor_price_comp_r1 > 0,
        competitor_price_comp_r1,
        NULL)
    ) AS price_{{ competitor }},
    {% endfor -%}
    MAX(price_helloprint) AS price_helloprint,
    MAX(price_helloprint_connect) AS price_helloprint_connect,
    MAX(cost_price) AS cost_price,
    MAX(supplier_price) AS supplier_price,
    MAX(carrier_cost) AS carrier_cost
    FROM map_filter_rename
    GROUP BY 1,2,3,4,5,6,7,8,9)

SELECT * FROM grouped 


{% endmacro %}
