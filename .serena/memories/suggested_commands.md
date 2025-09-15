# 推荐命令

## 基础操作命令
```bash
# 初始化数据库
go run cmd/main.go -cmd=init-db

# 发现交易对时间线
go run cmd/main.go -cmd=discover

# 运行数据加载
go run cmd/main.go -cmd=run

# 创建物化视图
go run cmd/main.go -cmd=create-views

# 填充物化视图历史数据
go run cmd/main.go -cmd=populate-views
```

## CSV导出命令
```bash
# 导出单个交易对的指定时间周期数据
go run cmd/main.go -cmd=export-csv -symbols=BTCUSDT -interval=1h

# 导出指定时间范围的数据
go run cmd/main.go -cmd=export-csv -symbols=BTCUSDT -interval=5m -start=2023-01-01 -end=2023-12-31

# 导出所有交易对的数据
go run cmd/main.go -cmd=export-csv -interval=1d -output=all_daily.csv
```

## 数据验证命令
```bash
# 数据完整性验证
go run cmd/main.go -cmd=verify-data

# 查看状态
go run cmd/main.go -cmd=status -detailed
```

## 脚本命令
```bash
# 启动系统
./start.sh

# 停止系统
./stop.sh

# 启动ClickHouse
./start_clickhouse.sh
```