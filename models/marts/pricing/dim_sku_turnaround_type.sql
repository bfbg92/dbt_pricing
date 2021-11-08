WITH turnaround_fastest AS (
  SELECT
    date_price_updated,
    country_name,
    company_name,
    LOWER(product_name) AS product_name,
    sku,
    LEFT(sku, LENGTH(sku) - instr(REVERSE(sku), '-', 1, 1)) AS sku_no_turnaround,
    'fastest' AS turnaround_type
  FROM
    {{ ref('stg_bigquery-data-analytics__pricing_monitoring_2') }}
),
turnaround_slowest AS (
  SELECT
    date_price_updated,
    country_name,
    company_name,
    LOWER(product_name) AS product_name,
    sku,
    LEFT(sku, LENGTH(sku) - instr(REVERSE(sku), '-', 1, 1)) AS sku_no_turnaround,
    'slowest' AS turnaround_type
  FROM
    {{ ref('stg_bigquery-data-analytics__pricing_monitoring_2') }}
),
union_data AS (
  SELECT
    *
  FROM
    turnaround_fastest
  UNION ALL
  SELECT
    *
  FROM
    turnaround_slowest
)
SELECT
  *
FROM
  union_data
