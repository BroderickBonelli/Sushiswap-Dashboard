WITH table1 AS (SELECT
    DATE_TRUNC('day', block_time)::date AS time,
    SUM(usd_amount) AS volume,
    COUNT(DISTINCT(tx_from))::numeric AS traders
    FROM dex."trades"
    WHERE project = 'Sushiswap'
    AND block_time < now() - interval '1 days'
    GROUP BY time
    ORDER BY time
),

sushi_price AS (SELECT
    DATE_TRUNC('day', minute) AS time,
    AVG(price) AS price
    FROM prices."usd"
    WHERE symbol = 'SUSHI'
    GROUP BY time
    ORDER BY time
),

sushi_1w_pct_change AS (
    WITH table_a AS (SELECT
        a.time, a.volume, a.traders,
        b.time AS time2, b.volume AS volume2, b.traders AS traders2,
        (((b.volume - a.volume)/a.volume)::float * 100) AS volume_1w_pct_change,
        (((b.traders - a.traders)/a.traders)::float * 100) AS traders_1w_pct_change
        FROM table1 a
        LEFT JOIN table1 b
        ON ((a.time + interval '7 days') = b.time)
        )
        
        SELECT time2 AS time, volume2 AS volume, volume_1w_pct_change, traders2 AS traders, traders_1w_pct_change
        FROM table_a
),

sushi_1mo_pct_change AS (
    WITH table_a AS (SELECT
        a.time, a.volume, a.traders,
        b.time AS time2, b.volume AS volume2, b.traders AS traders2,
        (((b.volume - a.volume)/a.volume)::float * 100) AS volume_1mo_pct_change,
        (((b.traders - a.traders)/a.traders)::float * 100) AS traders_1mo_pct_change
        FROM table1 a
        LEFT JOIN table1 b
        ON ((a.time + interval '30 days') = b.time)
        )
        
        SELECT time2 AS time, volume2 AS volume, volume_1mo_pct_change, traders2 AS traders, traders_1mo_pct_change
        FROM table_a
),

sushi_3mo_pct_change AS (
    WITH table_a AS (SELECT
        a.time, a.volume, a.traders,
        b.time AS time2, b.volume AS volume2, b.traders AS traders2,
        (((b.volume - a.volume)/a.volume)::float * 100) AS volume_3mo_pct_change,
        (((b.traders - a.traders)/a.traders)::float * 100) AS traders_3mo_pct_change
        FROM table1 a
        LEFT JOIN table1 b
        ON ((a.time + interval '90 days') = b.time)
        )
        
        SELECT time2 AS time, volume2 AS volume, volume_3mo_pct_change, traders2 AS traders, traders_3mo_pct_change
        FROM table_a
),

final_table AS (
    WITH ft1 AS (SELECT 
        table1.time, table1.volume, table1.traders, sushi_price.price
        FROM table1
        LEFT JOIN sushi_price
        ON (table1.time = sushi_price.time)
    ),
    
    ft2 AS (SELECT
        ft1.time, ft1.volume, ft1.traders, ft1.price, sushi_1w_pct_change.volume_1w_pct_change, sushi_1w_pct_change.traders_1w_pct_change
        FROM ft1
        LEFT JOIN sushi_1w_pct_change
        ON (ft1.time = sushi_1w_pct_change.time)
    ),
    
    ft3 AS (SELECT
        f.time, f.volume, f.traders, f.price, f.volume_1w_pct_change, f.traders_1w_pct_change, s.volume_1mo_pct_change, s.traders_1mo_pct_change
        FROM ft2 f
        LEFT JOIN sushi_1mo_pct_change s
        ON (f.time = s.time)
    ),
    
     ft4 AS (SELECT
        f.time, f.volume, f.traders, f.price, f.volume_1w_pct_change, f.traders_1w_pct_change, f.volume_1mo_pct_change, f.traders_1mo_pct_change, s.volume_3mo_pct_change, s.traders_3mo_pct_change
        FROM ft3 f
        LEFT JOIN sushi_3mo_pct_change s
        ON (f.time = s.time)
    )
    
    SELECT * FROM ft4
    
)


SELECT 
time AS "Date",
price AS "SUSHI Price",
volume AS "Volume",
volume_1w_pct_change AS "Volume 1w Δ (%)",
volume_1mo_pct_change AS "Volume 1mo Δ (%)",
volume_3mo_pct_change AS "Volume 3mo Δ (%)",
traders AS "Users",
traders_1w_pct_change AS "Users 1w Δ (%)",
traders_1mo_pct_change AS "Users 1mo Δ (%)",
traders_3mo_pct_change AS "Users 3mo Δ (%)"
FROM final_table
WHERE time > now() - interval '180 days'
ORDER BY time desc;