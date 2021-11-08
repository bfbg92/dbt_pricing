WITH turnaround_fastest AS (
  SELECT
    date_price_updated,
    country_name,
    company_name,
    LOWER(product_name) AS product_name,
    LEFT(sku, LENGTH(sku) - instr(REVERSE(sku), '-', 1, 1)) AS sku_no_turnaround,
    'fastest' AS turnaround_type,
    MIN(turnaround) AS turnaround
  FROM
    {{ ref('stg_bigquery-data-analytics__pricing_monitoring_2') }}
  WHERE turnaround <= 3
  GROUP BY 1,2,3,4,5,6
),
turnaround_slowest AS (
  SELECT
    date_price_updated,
    country_name,
    company_name,
    LOWER(product_name) AS product_name,
    LEFT(sku, LENGTH(sku) - instr(REVERSE(sku), '-', 1, 1)) AS sku_no_turnaround,
    'slowest' AS turnaround_type,
    MAX(turnaround) AS turnaround
  FROM
    {{ ref('stg_bigquery-data-analytics__pricing_monitoring_2') }}
  WHERE turnaround > 3
  GROUP BY 1,2,3,4,5,6

),
union_data AS (
  SELECT
    *, CONCAT(sku_no_turnaround,'-',turnaround) AS sku
  FROM
    turnaround_fastest
  UNION ALL
  SELECT
    *, CONCAT(sku_no_turnaround,'-',turnaround) AS sku
  FROM
    turnaround_slowest
)
SELECT
  *
FROM
  union_data
