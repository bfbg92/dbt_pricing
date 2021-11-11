{{ config(materialized='table') }}

/* input parameters */
{% set companies = ['helloprint', 'helloprint_connect', 'printoclock', 'realisaprint', 'flyeralarm'] %}


WITH 
join_turnaround_type AS (
SELECT 
  pm.date_price_updated,
  pm.country_name,
  pm.product_name,
  pm.sku,
  stt.sku_no_turnaround, --NULL if not slowest/fastest turnaround
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
  -- helloprint,
  price_helloprint,
  -- helloprint_connect,
  price_helloprint_connect,
  -- printoclock,
  price_printoclock,
  -- realisaprint
  price_realisaprint,
  -- flyeralarm
  price_flyeralarm
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
  {% for company in companies %}
     price_{{ company }},
     price_{{ company }},
     SUM(CASE WHEN price_{{ company }} IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) as {{ company }}_partition,
  {% endfor %}
FROM join_turnaround_type),
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
cost_price,
supplier_price,
carrier_cost,
-- helloprint,
CASE WHEN helloprint_partition = LAG(helloprint_partition, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) THEN FALSE ELSE TRUE END AS price_helloprint_is_real,
FIRST_VALUE(price_helloprint) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type, helloprint_partition ORDER BY date_price_updated ASC) as price_helloprint,
-- helloprint_connect,
CASE WHEN helloprint_connect_partition = LAG(helloprint_connect_partition, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) THEN FALSE ELSE TRUE END AS price_helloprint_connect_is_real,
FIRST_VALUE(price_helloprint_connect) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type, helloprint_connect_partition ORDER BY date_price_updated) as price_helloprint_connect,
-- printoclock,
CASE WHEN printoclock_partition = LAG(printoclock_partition, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) THEN FALSE ELSE TRUE END AS price_printoclock_is_real,
FIRST_VALUE(price_printoclock) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type, printoclock_partition ORDER BY date_price_updated) as price_printoclock,
-- realisaprint
CASE WHEN realisaprint_partition = LAG(realisaprint_partition, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) THEN FALSE ELSE TRUE END AS price_realisaprint_is_real,
FIRST_VALUE(price_realisaprint) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type, realisaprint_partition ORDER BY date_price_updated) as price_realisaprint,
-- flyeralarm
CASE WHEN flyeralarm_partition = LAG(flyeralarm_partition, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) THEN FALSE ELSE TRUE END AS price_flyeralarm_is_real,
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
cost_price,
supplier_price,
carrier_cost,
-- helloprint
price_helloprint_is_real,
price_helloprint,
LAG(price_helloprint, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) AS price_lag_helloprint,
-- helloprint_connect
price_helloprint_connect_is_real,
price_helloprint_connect,
LAG(price_helloprint_connect, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) AS price_lag_helloprint_connect,
-- printoclock
price_printoclock_is_real,
price_printoclock,
LAG(price_printoclock, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) AS price_lag_printoclock,
-- realisaprint
price_realisaprint_is_real,
price_realisaprint,
LAG(price_realisaprint, 1) OVER (PARTITION BY country_name, sku_no_turnaround, turnaround_type ORDER BY date_price_updated ASC) AS price_lag_realisaprint,
-- flyeralarm
price_flyeralarm_is_real,
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
cost_price,
supplier_price,
carrier_cost,
-- helloprint
price_helloprint_is_real,
price_helloprint,
CASE WHEN price_lag_helloprint IS NOT NULL THEN price_helloprint - price_lag_helloprint END as price_variation_helloprint,
-- helloprint_connect
price_helloprint_connect_is_real,
price_helloprint_connect,
CASE WHEN price_lag_helloprint_connect IS NOT NULL THEN price_helloprint_connect - price_lag_helloprint_connect END as price_variation_helloprint_connect,
-- printoclock
price_printoclock_is_real,
price_printoclock,
CASE WHEN price_lag_printoclock IS NOT NULL THEN price_printoclock - price_lag_printoclock END as price_variation_printoclock,
-- realisaprint
price_realisaprint_is_real,
price_realisaprint,
CASE WHEN price_lag_realisaprint IS NOT NULL THEN price_realisaprint - price_lag_realisaprint END as price_variation_realisaprint,
-- flyeralarm
price_flyeralarm_is_real,
price_flyeralarm,
CASE WHEN price_lag_flyeralarm IS NOT NULL THEN price_flyeralarm - price_lag_flyeralarm END as price_variation_flyeralarm

FROM price_variation