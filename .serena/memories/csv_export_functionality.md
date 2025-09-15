# CSV导出功能分析

## 核心功能确认

### ✅ 支持导出某个代币所有时间周期的数据为CSV文件
该项目完全支持此功能，具体特性：

**支持的时间周期**：
- 1m（1分钟）
- 5m（5分钟） 
- 15m（15分钟）
- 1h（1小时）
- 4h（4小时）
- 1d（1天）

**导出方式**：
```bash
# 导出BTCUSDT的1分钟数据
go run cmd/main.go -cmd=export-csv -symbols=BTCUSDT -interval=1m

# 导出BTCUSDT的5分钟数据
go run cmd/main.go -cmd=export-csv -symbols=BTCUSDT -interval=5m

# 导出BTCUSDT的4小时数据
go run cmd/main.go -cmd=export-csv -symbols=BTCUSDT -interval=4h
```

### ✅ 支持指定代币，指定时间周期导出CSV文件
完全支持，功能非常灵活：

**支持的参数**：
- `-symbols`: 指定交易对（如BTCUSDT,ETHUSDT）
- `-interval`: 指定时间周期（1m,5m,15m,1h,4h,1d）
- `-start`: 指定开始时间（YYYY-MM-DD或YYYY-MM）
- `-end`: 指定结束时间（YYYY-MM-DD或YYYY-MM）
- `-output`: 指定输出文件路径

**具体使用示例**：
```bash
# 导出BTCUSDT 2023年的1小时数据
go run cmd/main.go -cmd=export-csv -symbols=BTCUSDT -interval=1h -start=2023-01-01 -end=2023-12-31

# 导出多个交易对的5分钟数据到指定文件
go run cmd/main.go -cmd=export-csv -symbols=BTCUSDT,ETHUSDT -interval=5m -output=crypto_5m.csv

# 导出所有交易对的日线数据
go run cmd/main.go -cmd=export-csv -interval=1d -output=all_daily.csv
```

## 技术实现
- 使用`pkg/csvexport`包实现
- 支持流式处理大数据量
- 批量查询优化（10000条/批次）
- 标准CSV格式输出（ISO 8601时间戳）
- 自动文件名生成或自定义路径

## CSV文件格式
包含完整的OHLCV数据：
- timestamp, symbol, open, high, low, close
- volume, quote_volume, trades
- taker_buy_base_volume, taker_buy_quote_volume