WITH sushi_supply AS (
    WITH supply AS (SELECT 
        DATE_TRUNC('day', evt_block_time) AS time,
        SUM(value/1e18) OVER(ORDER BY evt_block_time) AS sushi_tokens
        FROM erc20."ERC20_evt_Transfer"
        WHERE contract_address = '\x6B3595068778DD592e39A122f4f5a5cF09C90fE2'
        AND "from" = '\x0000000000000000000000000000000000000000'
        AND evt_block_time < now() - interval '1 days'
        )

    SELECT DATE_TRUNC('day', time) AS time, AVG(sushi_tokens) AS sushi_tokens
    FROM supply
    GROUP BY time
    ORDER BY time
),

price AS (SELECT
    DATE_TRUNC('day', minute) AS time,
    AVG(price) AS price
    FROM prices."usd"
    WHERE symbol = 'SUSHI'
    AND minute < now() - interval '1 days'
    GROUP BY time
    ORDER BY time
),

cap AS (SELECT 
    sushi_supply.time, sushi_supply.sushi_tokens * price.price AS market_cap
    FROM sushi_supply
    JOIN price
    ON (sushi_supply.time = price.time)
    ORDER BY time
),

sush AS (SELECT
    DATE_TRUNC('day', block_time) AS time,
    SUM(usd_amount) AS volume,
    SUM(usd_amount) * .003 AS swap_fees,
    COUNT(DISTINCT(tx_from)) AS traders
    FROM dex."trades"
    WHERE project = 'Sushiswap'
    AND block_time < now() - interval '1 days'
    GROUP BY time
    ORDER BY time
),

pairs AS (SELECT
    DATE_TRUNC('day', evt_block_time) AS time,
    SUM(COUNT("pair")) OVER (ORDER BY DATE_TRUNC('day', evt_block_time)) AS total_pairs
    FROM sushi."Factory_evt_PairCreated"
    GROUP BY time
    ORDER BY time
),

tvl AS (SELECT 
    DATE_TRUNC('day', day) AS time, 
    sum(token_usd_amount) AS sushiswap_tvl
    FROM dex."liquidity"
    WHERE project = 'Sushiswap'
    GROUP BY time
    ORDER BY time
),

table1 AS (SELECT
    sushi_supply.time, sushi_supply.sushi_tokens, price.price
    FROM sushi_supply
    LEFT JOIN price
    ON (sushi_supply.time = price.time)
),

table2 AS (SELECT
    table1.time, table1.sushi_tokens, table1.price, cap.market_cap
    FROM table1
    LEFT JOIN cap
    ON (table1.time = cap.time)
),

table3 AS (SELECT
    table2.time, table2.sushi_tokens, table2.price, table2.market_cap, sush.volume, sush.swap_fees, sush.traders
    FROM table2
    LEFT JOIN sush
    ON (table2.time = sush.time)
),

table4 AS (SELECT
    table3.time, table3.sushi_tokens, table3.price, table3.market_cap, table3.volume, table3.swap_fees, table3.traders, pairs.total_pairs
    FROM table3
    LEFT JOIN pairs
    ON (table3.time = pairs.time)
),

table5 AS (SELECT
    table4.time, table4.sushi_tokens, table4.price, table4.market_cap, table4.volume, table4.swap_fees, table4.traders, table4.total_pairs, tvl.sushiswap_tvl
    FROM table4
    LEFT JOIN tvl
    ON (table4.time = tvl.time)
)

SELECT 
time AS "Date",
sushi_tokens AS "SUSHI Tokens",
price AS "SUSHI Price",
market_cap AS "Market Cap",
volume AS "SushiSwap Volume",
swap_fees AS "Total Swap Fees",
traders AS "# of Users",
total_pairs AS "Total Pairs",
sushiswap_tvl AS "TVL"
FROM table5
ORDER BY "Date" desc;