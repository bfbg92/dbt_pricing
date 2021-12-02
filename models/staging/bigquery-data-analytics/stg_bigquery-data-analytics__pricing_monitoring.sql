
/* input parameters */
{% set competitors = var('pricing_competitors') %}
{% set countries = var('pricing_countries') %}
{% set products = var('pricing_products') %}


WITH 

filtered_renamed AS (
  SELECT
    CAST(time_price_updated AS DATE) AS spider_run_at,
    country_name,
    LOWER(product_name) AS product_name,
    sku,
    quantity,
    turnaround,
    material,
    size,
    cover,
    finishing,
   /* loop through competitors */
   {% for competitor in competitors -%}
    MAX(
      if(
        competitor_name = '{{ competitor }}'
        AND competitor_price_comp_r1 > 0,
        competitor_price_comp_r1,
        NULL)
    ) AS price_{{ competitor }}
   {%- if not loop.last %},{% endif %}
   {% endfor -%}
    MAX(salesprice_comp_r1) AS price_helloprint,
    MAX(salesprice_comp_all) AS price_helloprint_connect,
    MAX(cost_price) AS cost_price,
    MAX(supplier_price) AS supplier_price,
    MAX(carrier_cost) AS carrier_cost
    FROM {{ source('bigquery-data-analytics', 'pricing_monitoring') }}
    WHERE
    country_name IN ('{{ "','".join(countries) }}')
    AND product_name IN ('{{ "','".join(products) }}')
    AND competitor_name IN ('{{ "','".join(competitors) }}')

    
    AND salesprice_comp_r1 IS NOT NULL
    AND salesprice_comp_r1 > 0
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    )

SELECT * FROM filtered_renamed 
