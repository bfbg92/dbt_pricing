{{ config(materialized='table') }}

WITH 
inner_join_turnaround_type AS (
SELECT 
  fact.date_price_updated,
  fact.country_name,
  fact.product_name,
  fact.sku,
  dim.sku_no_turnaround,
  fact.quantity,
  fact.turnaround,
  dim.turnaround_type,
  fact.material,
  fact.size,
  fact.cover,
  fact.finishing,
-- helloprint,
   SUM(CASE WHEN fact.company_name = 'Helloprint' THEN fact.price ELSE NULL END) as price_helloprint,
-- helloprint_connect,
   SUM(CASE WHEN fact.company_name = 'Helloprint Connect' THEN fact.price ELSE NULL END) as price_helloprint_connect,
-- printoclock,
   SUM(CASE WHEN fact.company_name = 'printoclock' THEN fact.price ELSE NULL END) as price_printoclock,
-- realisaprint
   SUM(CASE WHEN fact.company_name = 'realisaprint' THEN fact.price ELSE NULL END) as price_realisaprint,
-- flyeralarm
   SUM(CASE WHEN fact.company_name = 'flyeralarm' THEN fact.price ELSE NULL END) as price_flyeralarm
FROM {{ ref('stg_bigquery-data-analytics__pricing_monitoring_2') }} fact
INNER JOIN {{ ref('dim_sku_turnaround_type') }} dim ON 
  fact.date_price_updated = dim.date_price_updated AND
  fact.country_name = dim.country_name AND
  fact.product_name = dim.product_name AND
  fact.sku = dim.sku AND
  fact.company_name = dim.company_name
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12),

fill_nulls_temp as (
SELECT 
  date_price_updated,
  country_name,
  product_name,
  sku,
  sku_no_turnaround,
  company_name,
  quantity,
  turnaround,
  turnaround_type,
  material,
  size,
  cover,
  finishing,
-- helloprint,
   price_helloprint,
   SUM(CASE WHEN price_helloprint IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) as helloprint_partition,
-- helloprint_connect,
   price_helloprint_connect,
   SUM(CASE WHEN price_helloprint_connect IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) as helloprint_connect_partition,
-- printoclock,
   price_printoclock,
   SUM(CASE WHEN price_printoclock IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) as printoclock_partition,
-- realisaprint
   price_realisaprint,
   SUM(CASE WHEN price_realisaprint IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) as realisaprint_partition,
-- flyeralarm
   price_flyeralarm,
   SUM(CASE WHEN price_flyeralarm IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) as flyeralarm_partition
FROM inner_join_turnaround_type),
-----------------------------------------------------------------------------------------------------

-- At this step, null values of competitor price columns are filled with previous non-null price.
fill_nulls AS (
SELECT
-- Commun Dimensions
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
-- helloprint,
FIRST_VALUE(price_helloprint) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type, helloprint_partition ORDER BY date_price_updated ASC) as price_helloprint,
-- helloprint_connect,
FIRST_VALUE(price_helloprint_connect) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type, helloprint_connect_partition ORDER BY date_price_updated) as price_helloprint_connect,
-- printoclock,
FIRST_VALUE(price_printoclock) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type, printoclock_partition ORDER BY date_price_updated) as price_printoclock,
-- realisaprint
FIRST_VALUE(price_realisaprint) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type, realisaprint_partition ORDER BY date_price_updated) as price_realisaprint,
-- flyeralarm
FIRST_VALUE(price_flyeralarm) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type, flyeralarm_partition ORDER BY date_price_updated) as price_flyeralarm,
FROM fill_nulls_temp),
-----------------------------------------------------------------------------------------------------

-- At this step, previous competitor price columns are generated (price_lag).
price_variation AS (
SELECT
-- Commun Dimensions
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
-- helloprint
price_helloprint,
LAG(price_helloprint, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) AS price_lag_helloprint,
-- helloprint_connect
price_helloprint_connect,
LAG(price_helloprint_connect, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) AS price_lag_helloprint_connect,
-- printoclock
price_printoclock,
LAG(price_printoclock, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) AS price_lag_printoclock,
-- realisaprint
price_realisaprint,
LAG(price_realisaprint, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) AS price_lag_realisaprint,
-- flyeralarm
price_flyeralarm,
LAG(price_flyeralarm, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) AS price_lag_flyeralarm
FROM fill_nulls)
-----------------------------------------------------------------------------------------------------

-- Finally, price variations are computed.
SELECT
-- Commun Dimensions
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