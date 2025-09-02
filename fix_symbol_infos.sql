-- 修复symbol_infos表，为所有有数据的交易对添加记录
-- 这个脚本会为klines_1m表中存在但symbol_infos表中缺失的交易对创建记录

INSERT INTO symbol_infos (
    symbol, 
    status, 
    base_asset, 
    quote_asset, 
    earliest_date, 
    latest_date, 
    total_months, 
    data_status, 
    created_at, 
    updated_at
)
SELECT 
    symbol,
    'TRADING' as status,
    substring(symbol, 1, length(symbol) - 4) as base_asset,  -- 去掉USDT后缀
    'USDT' as quote_asset,
    toDate(MIN(open_time)) as earliest_date,
    toDate(MAX(open_time)) as latest_date,
    dateDiff('month', toDate(MIN(open_time)), toDate(MAX(open_time))) + 1 as total_months,
    'imported' as data_status,
    now() as created_at,
    now() as updated_at
FROM klines_1m 
WHERE symbol NOT IN (SELECT symbol FROM symbol_infos)
GROUP BY symbol
ORDER BY symbol;

-- 验证插入结果
SELECT COUNT(*) as total_symbols_after_fix FROM symbol_infos;

-- 显示前10个新增的记录
SELECT symbol, total_months, earliest_date, latest_date 
FROM symbol_infos 
WHERE data_status = 'imported'
ORDER BY symbol 
LIMIT 10;