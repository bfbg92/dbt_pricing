
/*
    Pricing Dataset - France - Flyers3
    Model: clean_data
    Descrpition:
        At this stage, only a subset of the data is extracted and processed.
        Also, irrelevant columns and rows are discarded
        The outcome is a cleanner version of the source data.        
*/

{{ config(materialized='table') }}

SELECT
  --Dimensions
  CAST(time_price_updated AS DATE) AS date_price_updated,
  country_name,
  LOWER(product_name) AS product_name,
  sku,
  quantity,
  turnaround,
  material,
  size,
  cover,
  finishing,
  --Metrics
  max(IF(competitor_name='printoclock' AND competitor_price_comp_r1>0,competitor_price_comp_r1,NULL)) AS price_printoclock,
  max(IF(competitor_name='realisaprint' AND competitor_price_comp_r1>0,competitor_price_comp_r1,NULL)) AS price_realisaprint,
  max(IF(competitor_name='flyeralarm' AND competitor_price_comp_r1>0,competitor_price_comp_r1,NULL)) AS price_flyeralarm,
  max(salesprice_comp_r1) AS price_helloprint,
  max(salesprice_comp_all) AS price_helloprint_connect,
  max(cost_price) AS cost_price,
  max(supplier_price) AS supplier_price,
  max(carrier_cost) AS carrier_cost
FROM
  `helloprint-data-analytics-live.silver_raw.pricing_monitoring_data_staging`
WHERE
  country_name = 'France'
  AND product_name = 'Flyers3'
  AND competitor_name IN ('printoclock', 'realisaprint', 'flyeralarm')
  AND salesprice_comp_r1 IS NOT NULL
  AND salesprice_comp_r1 > 0
  AND salesprice_comp_all IS NOT NULL
  AND salesprice_comp_all > 0
  AND competitor_price_comp_r1 IS NOT NULL
  AND competitor_price_comp_r1 > 0
GROUP BY 1,2,3,4,5,6,7,8,9,10