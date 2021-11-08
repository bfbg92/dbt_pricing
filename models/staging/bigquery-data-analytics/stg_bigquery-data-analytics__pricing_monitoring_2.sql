{{ config(materialized='table') }}


WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'bigquery-data-analytics',
            'pricing_monitoring'
        ) }}
),
competitor_filtered_renamed AS (
    SELECT
        CAST(
            time_price_updated AS DATE
        ) AS date_price_updated,
        country_name,
        LOWER(product_name) AS product_name,
        sku,
        quantity,
        turnaround,
        material,
        size,
        cover,
        finishing,
        competitor_name as company_name,
        MAX(competitor_price_comp_r1) AS price
    FROM
        source
    WHERE
        country_name = 'France'
        AND product_name = 'Flyers3'
        AND competitor_name IN (
            'printoclock',
            'realisaprint',
            'flyeralarm'
        )
        AND competitor_price_comp_r1 IS NOT NULL
        AND competitor_price_comp_r1 > 0
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11
),
helloprint_filtered_renamed AS (
    SELECT
        CAST(
            time_price_updated AS DATE
        ) AS date_price_updated,
        country_name,
        LOWER(product_name) AS product_name,
        sku,
        quantity,
        turnaround,
        material,
        size,
        cover,
        finishing,
        'Helloprint' as company_name,
        MAX(salesprice_comp_r1) AS price
    FROM
        source
    WHERE
        country_name = 'France'
        AND product_name = 'Flyers3'
        AND competitor_name IN (
            'printoclock',
            'realisaprint',
            'flyeralarm'
        )
        AND salesprice_comp_r1 IS NOT NULL
        AND salesprice_comp_r1 > 0
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11
),
helloprint_connect_filtered_renamed AS (
    SELECT
        CAST(
            time_price_updated AS DATE
        ) AS date_price_updated,
        country_name,
        LOWER(product_name) AS product_name,
        sku,
        quantity,
        turnaround,
        material,
        size,
        cover,
        finishing,
        'Helloprint Connect' as company_name,
        MAX(salesprice_comp_all) AS price
    FROM
        source
    WHERE
        country_name = 'France'
        AND product_name = 'Flyers3'
        AND competitor_name IN (
            'printoclock',
            'realisaprint',
            'flyeralarm'
        )
        AND salesprice_comp_all IS NOT NULL
        AND salesprice_comp_all > 0

    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11
),
union_data AS (
    SELECT * FROM competitor_filtered_renamed UNION ALL
    SELECT * FROM helloprint_filtered_renamed UNION ALL
    SELECT * FROM helloprint_connect_filtered_renamed
)
SELECT
    *
FROM
    union_data
