-- Initialize data4BT database and user permissions
CREATE DATABASE IF NOT EXISTS data4BT;

-- Grant permissions to default user
GRANT ALL ON data4BT.* TO default;

-- Create the main klines table
CREATE TABLE IF NOT EXISTS data4BT.klines_1m (
    symbol String,
    open_time DateTime64(3),
    close_time DateTime64(3),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    quote_asset_volume Float64,
    number_of_trades Int64,
    taker_buy_base_volume Float64,
    taker_buy_quote_volume Float64,
    interval String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (symbol, toYYYYMM(open_time))
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;