WITH turnaround_slowest AS (
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
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6
),
turnaround_fastest AS (
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
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6
),
union_data AS (
  SELECT
    *,
    CONCAT(
      sku_no_turnaround,
      '-',
      turnaround
    ) AS sku
  FROM
    turnaround_slowest
  UNION ALL
  SELECT
    *,
    CONCAT(
      sku_no_turnaround,
      '-',
      turnaround
    ) AS sku
  FROM
    turnaround_fastest tf
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM
        turnaround_slowest ts
      WHERE
        tf.date_price_updated = ts.date_price_updated
        AND tf.country_name = ts.country_name
        AND tf.company_name = ts.company_name
        AND tf.product_name = ts.product_name
        AND tf.sku_no_turnaround = ts.sku_no_turnaround
        AND
    )
  SELECT
    *
  FROM
    union_data
