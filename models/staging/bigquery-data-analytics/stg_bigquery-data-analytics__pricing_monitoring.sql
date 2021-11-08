WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'bigquery-data-analytics',
            'pricing_monitoring'
        ) }}
),
filtered_renamed AS (
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
        MAX(
            if(
                competitor_name = 'printoclock'
                AND competitor_price_comp_r1 > 0,
                competitor_price_comp_r1,
                NULL
            )
        ) AS price_printoclock,
        MAX(
            if(
                competitor_name = 'realisaprint'
                AND competitor_price_comp_r1 > 0,
                competitor_price_comp_r1,
                NULL
            )
        ) AS price_realisaprint,
        MAX(
            if(
                competitor_name = 'flyeralarm'
                AND competitor_price_comp_r1 > 0,
                competitor_price_comp_r1,
                NULL
            )
        ) AS price_flyeralarm,
        MAX(salesprice_comp_r1) AS price_helloprint,
        MAX(salesprice_comp_all) AS price_helloprint_connect,
        MAX(cost_price) AS cost_price,
        MAX(supplier_price) AS supplier_price,
        MAX(carrier_cost) AS carrier_cost
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
        AND salesprice_comp_all IS NOT NULL
        AND salesprice_comp_all > 0
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
        10
)
SELECT
    *
FROM
    filtered_renamed
