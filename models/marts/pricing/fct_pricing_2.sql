{{ config(materialized='table') }}

WITH fill_nulls_temp as (
SELECT 
  date_price_updated,
  country_name,
  product_name,
  sku,
  company_name,
  quantity,
  turnaround,
  turnaround_type,
  material,
  size,
  cover,
  finishing,
-- helloprint,
   SUM(CASE WHEN company_name = 'Helloprint' THEN price ELSE NULL END) as price_helloprint,
   SUM(CASE WHEN company_name = 'Helloprint' AND price IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, product_name, sku ORDER BY date_price_updated ASC) as helloprint_partition,
-- helloprint_connect,
   SUM(CASE WHEN company_name = 'Helloprint Connect' THEN price ELSE NULL END) as price_helloprint_connect,
   SUM(CASE WHEN company_name = 'Helloprint Connect' AND price IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, product_name, sku ORDER BY date_price_updated ASC) as helloprint_connect_partition,
-- printoclock,
   SUM(CASE WHEN company_name = 'printoclock' THEN price ELSE NULL END) as price_printoclock,
   SUM(CASE WHEN company_name = 'printoclock' AND price IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, product_name, sku ORDER BY date_price_updated ASC) as printoclock_partition,
-- realisaprint
   SUM(CASE WHEN company_name = 'realisaprint' THEN price ELSE NULL END) as price_realisaprint,
   SUM(CASE WHEN company_name = 'realisaprint' AND price IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, product_name, sku ORDER BY date_price_updated ASC) as realisaprint_partition,
-- flyeralarm
   SUM(CASE WHEN company_name = 'flyeralarm' THEN price ELSE NULL END) as price_flyeralarm,
   SUM(CASE WHEN company_name = 'flyeralarm' AND price IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, product_name, sku ORDER BY date_price_updated ASC) as flyeralarm_partition
FROM {{ ref('stg_bigquery-data-analytics__pricing_monitoring_2') }} fact
INNER JOIN {{ ref('dim_sku_turnaround_type') }} dim ON 
  fact.date_price_updated = dim.date_price_updated AND
  fact.country_name = dim.country_name AND
  fact.product_name = dim.product_name AND
  fact.sku = dim.sku AND
  fact.company_name = dim.company_name AND

ORDER BY SKU ASC, date_price_updated ASC),
-----------------------------------------------------------------------------------------------------

-- At this step, null values of competitor price columns are filled with previous non-null price.
fill_nulls AS (
SELECT
-- Commun Dimensions
date_price_updated,
country_name,
product_name,
sku,
quantity,
turnaround,
material,
size,
cover,
finishing,
-- helloprint,
FIRST_VALUE(price_helloprint) OVER (PARTITION BY sku, helloprint_partition ORDER BY date_price_updated) as price_helloprint,
-- helloprint_connect,
FIRST_VALUE(price_helloprint_connect) OVER (PARTITION BY sku, helloprint_connect_partition ORDER BY date_price_updated) as price_helloprint_connect,
-- printoclock,
FIRST_VALUE(price_printoclock) OVER (PARTITION BY sku, printoclock_partition ORDER BY date_price_updated) as price_printoclock,
-- realisaprint
FIRST_VALUE(price_realisaprint) OVER (PARTITION BY sku, realisaprint_partition ORDER BY date_price_updated) as price_realisaprint,
-- flyeralarm
FIRST_VALUE(price_flyeralarm) OVER (PARTITION BY sku, flyeralarm_partition ORDER BY date_price_updated) as price_flyeralarm,
FROM fill_nulls_temp
ORDER BY SKU ASC, date_price_updated ASC),
-----------------------------------------------------------------------------------------------------

-- At this step, previous competitor price columns are generated (price_lag).
price_variation AS (
SELECT
-- Commun Dimensions
date_price_updated,
country_name,
product_name,
sku,
quantity,
turnaround,
material,
size,
cover,
finishing,
-- helloprint
price_helloprint,
LAG(price_helloprint, 1) OVER (PARTITION BY sku ORDER BY date_price_updated  ASC) AS price_lag_helloprint,
-- helloprint_connect
price_helloprint_connect,
LAG(price_helloprint_connect, 1) OVER (PARTITION BY sku ORDER BY date_price_updated  ASC) AS price_lag_helloprint_connect,
-- printoclock
price_printoclock,
LAG(price_printoclock, 1) OVER (PARTITION BY sku ORDER BY date_price_updated  ASC) AS price_lag_printoclock,
-- realisaprint
price_realisaprint,
LAG(price_realisaprint, 1) OVER (PARTITION BY sku ORDER BY date_price_updated  ASC) AS price_lag_realisaprint,
-- flyeralarm
price_flyeralarm,
LAG(price_flyeralarm, 1) OVER (PARTITION BY sku ORDER BY date_price_updated  ASC) AS price_lag_flyeralarm
FROM fill_nulls)
-----------------------------------------------------------------------------------------------------

-- Finally, price variations are computed.
SELECT
-- Commun Dimensions
date_price_updated,
country_name,
product_name,
sku,
quantity,
turnaround,
material,
size,
cover,
finishing,
-- helloprint
price_helloprint,
CASE WHEN price_lag_helloprint IS NOT NULL THEN price_helloprint - price_lag_helloprint END as price_variation_helloprint,
-- helloprint_connect
price_helloprint_connect,
CASE WHEN price_lag_helloprint_connect IS NOT NULL THEN price_helloprint_connect - price_lag_helloprint_connect END as price_variation_helloprint_connect,
-- printoclock
price_printoclock,
CASE WHEN price_lag_printoclock IS NOT NULL THEN price_printoclock - price_lag_printoclock END as price_variation_printoclock,
-- realisaprint
price_realisaprint,
CASE WHEN price_lag_realisaprint IS NOT NULL THEN price_realisaprint - price_lag_realisaprint END as price_variation_realisaprint,
-- flyeralarm
price_flyeralarm,
CASE WHEN price_lag_flyeralarm IS NOT NULL THEN price_flyeralarm - price_lag_flyeralarm END as price_variation_flyeralarm

FROM price_variation