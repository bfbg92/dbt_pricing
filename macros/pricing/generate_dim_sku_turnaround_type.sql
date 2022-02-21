{% macro generate_dim_sku_turnaround_type(country) %}

{% set stg_pricing_monitoring_country = 'stg_bigquery-data-analytics__pricing_monitoring_' ~ country %}


WITH

turnaround_slowest AS (
  SELECT
    spider_update_at,
    product_name,
    LEFT(sku, LENGTH(sku) - instr(REVERSE(sku), '-', 1, 1)) AS sku_no_turnaround,
    'slowest' AS turnaround_type,
    MAX(turnaround) AS turnaround
  FROM {{ ref(stg_pricing_monitoring_country) }} pm
  GROUP BY 1,2,3,4
  ),

turnaround_fastest AS (
  SELECT
    spider_update_at,
    product_name,
    LEFT(sku, LENGTH(sku) - instr(REVERSE(sku), '-', 1, 1)) AS sku_no_turnaround,
    'fastest' AS turnaround_type,
    MIN(turnaround) AS turnaround
  FROM {{ ref(stg_pricing_monitoring_country) }} 
  GROUP BY 1,2,3,4
  ),

union_data AS (
  SELECT
    *,
    CONCAT(sku_no_turnaround, '-', turnaround) AS sku
  FROM turnaround_slowest
  UNION ALL
  SELECT
    *,
    CONCAT(sku_no_turnaround, '-', turnaround) AS sku
  FROM turnaround_fastest tf
  WHERE
    NOT EXISTS(
      SELECT 1
      FROM turnaround_slowest ts
      WHERE
        tf.spider_update_at = ts.spider_update_at
        AND tf.product_name = ts.product_name
        AND CONCAT(tf.sku_no_turnaround, '-', tf.turnaround) = CONCAT(ts.sku_no_turnaround, '-', ts.turnaround)
        )
  )

SELECT * FROM union_data


{% endmacro %}