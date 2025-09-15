# Binance Data Loader

一个高性能的币安K线数据下载和处理系统，支持从币安官方数据源下载历史K线数据，解析CSV文件，验证数据完整性，并将数据导入到ClickHouse数据库中。系统还支持创建多时间周期的物化视图，便于后续的数据分析和查询。

## 功能特性

### 核心功能

- 🚀 **高性能下载**: 支持并发下载，自动重试机制
- 📊 **数据验证**: 完整的CSV数据验证和质量检查
- 🗄️ **ClickHouse集成**: 高效的列式数据库存储
- ⏱️ **物化视图**: 自动创建5m、15m、1h、4h、1d等多时间周期视图
- 📈 **进度监控**: 实时进度报告和Web仪表板
- 🔄 **状态管理**: 支持断点续传和增量更新
- 📝 **结构化日志**: 详细的操作日志和性能指标
- 🛑 **智能信号处理**: 优雅的进程管理和快速响应Ctrl+C
- 🔧 **进程管理**: 智能进程查找、优雅关闭和强制终止机制

### 技术特性

- **并发处理**: 支持多协程并发下载和处理
- **内存优化**: 流式处理大文件，控制内存使用
- **错误恢复**: 自动重试和错误处理机制
- **配置灵活**: 支持YAML配置文件
- **监控友好**: 提供HTTP API和Web界面
- **信号处理**: 完善的SIGTERM/SIGINT处理，支持优雅关闭
- **进程管理**: 多种进程查找方式（PID文件、进程名、端口）
- **脚本集成**: 改进的启动和停止脚本，支持前后台运行模式

## 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Scheduler     │───▶│    Downloader   │───▶│     Parser      │
│   (调度器)       │    │    (下载器)      │    │    (解析器)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ State Manager   │    │   Importer      │    │  ClickHouse     │
│  (状态管理)      │    │   (导入器)       │    │   Repository    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│Progress Reporter│    │   Buffer        │    │ Materialized    │
│  (进度报告)      │    │   (缓冲区)       │    │    Views        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 快速开始

### 1. 环境要求

- Go 1.19+
- ClickHouse 21.8+
- 网络连接（访问币安数据源）

### 2. 安装依赖

```bash
go mod download
```

### 3. 配置文件

项目支持多环境配置，根据不同的使用场景选择合适的配置：

#### 默认配置
```bash
cp config.yml.example config.yml
```

#### 开发环境配置
```bash
cp configs/config-dev.yml.example configs/config-dev.yml
# 编辑配置文件以适应你的开发环境
vim configs/config-dev.yml
```

#### 生产环境配置
```bash
cp configs/config-prod.yml.example configs/config-prod.yml
# 编辑配置文件以适应你的生产环境
vim configs/config-prod.yml
```

#### 主要配置项：

```yaml
# ClickHouse数据库配置
database:
  hosts: ["localhost:9000"]
  database: "binance_data"
  username: "default"
  password: ""

# 调度器配置
scheduler:
  end_date: ""
  batch_days: 7
  concurrent_symbols: 5
```

#### 环境配置差异

| 配置项 | 开发环境 | 生产环境 |
|--------|----------|----------|
| 日志级别 | debug | warn |
| 日志格式 | json | json |
| 下载并发数 | 10 | 20 |
| 解析并发数 | 5 | 10 |
| 批次大小 | 10000 | 50000 |
| 超时时间 | 120s | 120s |
| 重试次数 | 5 | 10 |

**配置文件优先**: 系统完全基于配置文件工作，无需设置环境变量。详细说明请参考 [docs/DEV_ENVIRONMENT.md](docs/DEV_ENVIRONMENT.md)

### 4. 初始化数据库

```bash
go run cmd/main.go -cmd=init-db
```

### 5. 发现交易对

在开始下载数据之前，建议先发现可用的交易对时间线：

```bash
# 发现所有USDT交易对的时间线信息
go run cmd/main.go -cmd=discover

# 或者只发现特定交易对
go run cmd/main.go -cmd=discover -symbols=BTCUSDT,ETHUSDT
```

### 6. 启动ClickHouse数据库

在运行主程序之前，需要先启动ClickHouse数据库：

```bash
# 使用Docker Compose启动ClickHouse
docker compose up -d clickhouse

# 验证ClickHouse是否正常运行
docker compose ps clickhouse

# 测试连接
curl http://localhost:8123/ping
```

#### 故障排除

如果遇到认证失败错误（Authentication failed），请使用提供的重启脚本：

```bash
# 运行重启脚本（会自动重启容器并测试连接）
./restart_clickhouse.sh
```

或者手动重启：

```bash
# 停止并重新启动ClickHouse
docker compose down clickhouse
docker compose up -d clickhouse

# 等待30-60秒让容器完全启动
sleep 30

# 测试连接
curl http://localhost:8123/ping
```

### 7. 运行数据加载

#### 使用改进的启动脚本 (推荐)

```bash
# 前台运行 (默认配置)
./start.sh

# 后台运行 (默认配置)
./start.sh --background

# 开发环境模式
./start.sh --dev

# 生产环境模式
./start.sh --prod

# 生产环境后台运行
./start.sh --prod --background

# 详细输出模式
./start.sh --verbose
```

#### 直接使用Go命令

```bash
# 复制配置文件到项目根目录（如果还没有的话）
cp configs/config.yml config.yml

# 运行数据加载器（会根据发现的时间线自动下载数据）
go run cmd/main.go -cmd=run

# 加载到指定结束日期
go run cmd/main.go -cmd=run -end=2024-01-31

# 加载指定交易对的数据
go run cmd/main.go -cmd=run -symbols=BTCUSDT,ETHUSDT
```

### 8. 物化视图管理

#### 8.1 创建物化视图

```bash
# 创建所有时间间隔的物化视图 (5m, 15m, 1h, 4h, 1d)
go run cmd/main.go -cmd=create-views -config=configs/config-dev.yml
```

#### 8.2 填充物化视图历史数据

**注意**: ClickHouse物化视图只处理创建后插入的新数据，不会自动包含历史数据。需要手动填充历史数据：

```bash
# 填充所有物化视图的历史数据（支持分批处理，避免内存溢出）
go run cmd/main.go -cmd=populate-views -config=configs/config-dev.yml
```

**填充过程特点**：
- ✅ **自动分批处理**: 按月份分批处理，避免大数据量导致内存溢出
- ⚡ **智能优化**: 自动设置ClickHouse分区限制，支持大量交易对并发处理
- 🔄 **断点续传**: 支持中断后继续处理，已处理的数据不会重复
- 📊 **实时进度**: 显示详细的处理进度和批次信息
- 🛡️ **数据安全**: 使用相同的聚合逻辑，确保数据一致性

**处理时间估算**（336M条基础数据）：
- 5分钟数据：约2-3分钟处理完成
- 15分钟数据：约1-2分钟处理完成
- 1小时数据：约30-60秒处理完成
- 4小时和1天数据：约10-30秒处理完成

#### 8.3 验证物化视图数据

```bash
# 检查物化视图数据量
docker exec data4bt-clickhouse clickhouse-client --user=default --password=123456 --query "
SELECT
    'klines_5m' as table, count(*) as row_count FROM data4BT.klines_5m
UNION ALL SELECT
    'klines_15m' as table, count(*) as row_count FROM data4BT.klines_15m
UNION ALL SELECT
    'klines_1h' as table, count(*) as row_count FROM data4BT.klines_1h
UNION ALL SELECT
    'klines_4h' as table, count(*) as row_count FROM data4BT.klines_4h
UNION ALL SELECT
    'klines_1d' as table, count(*) as row_count FROM data4BT.klines_1d
ORDER BY table"

# 验证数据聚合正确性（以BTCUSDT为例）
docker exec data4bt-clickhouse clickhouse-client --user=default --password=123456 --query "
SELECT
    symbol,
    count(*) as records,
    min(open_time) as earliest,
    max(open_time) as latest
FROM data4BT.klines_5m
WHERE symbol IN ('BTCUSDT', 'ETHUSDT')
GROUP BY symbol
ORDER BY symbol"
```

#### 8.4 常见问题解决

**内存不足错误**：
```
Memory limit exceeded: would use X.XX GiB
```
**解决方案**: 系统已实现自动分批处理，如仍遇到此问题，可重新运行命令继续处理。

**分区过多错误**：
```
Too many partitions for single INSERT block (more than 100)
```
**解决方案**: 系统已自动设置 `max_partitions_per_insert_block = 1000`，支持大量交易对并发处理。

**进度恢复**：
如果处理过程中断，重新运行 `populate-views` 命令，系统会自动跳过已处理的数据，从中断处继续。

### 9. 停止数据加载

#### 使用改进的停止脚本 (推荐)

```bash
# 智能停止所有Data4BT相关进程
./stop.sh

# 详细输出模式
./stop.sh --verbose

# 预览模式 (查看将要停止的进程，不实际执行)
./stop.sh --dry-run

# 自定义超时时间
./stop.sh --timeout 60 --force-timeout 15
```

#### 手动停止

```bash
# 使用Ctrl+C停止前台运行的程序
# 程序会优雅地处理信号并安全退出

# 或者查找并手动终止进程
pgrep -f "go run.*cmd/main.go" | xargs kill -TERM
```

### 10. 数据验证

在数据下载完成后，建议进行数据验证以确保数据质量：

```bash
# 综合数据验证（推荐）- 同时检查完整性和质量
go run cmd/main.go -cmd=verify-data -config=configs/config-prod.yml

# 验证特定交易对的数据
go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT,ETHUSDT

# 基础数据完整性验证
go run cmd/main.go -cmd=validate
```

### 11. 定期维护

建议定期运行以下命令来维护数据的完整性：

```bash
# 每月更新交易对时间范围（当有新的月度数据时）
go run cmd/main.go -cmd=update-ranges

# 查看数据库中的交易对状态
go run cmd/main.go -cmd=list-symbols

# 检查下载状态和进度
go run cmd/main.go -cmd=status -detailed

# 定期进行数据质量检查
go run cmd/main.go -cmd=verify-data
```

### 12. 脚本速查

- `./start.sh` 启动数据加载器（支持 `--background`、`--dev`、`--prod`、`--verbose`）
- `./stop.sh` 智能停止相关进程（支持 `--dry-run`、`--verbose`、自定义超时）
- `./status.sh` 查看下载与处理状态（支持 `-d`/`-detailed` 更详细输出）
- `./restart_clickhouse.sh` 重启并自检 ClickHouse 容器
- `./check_status.sh` 快速系统状态体检

### 13. 数据库清理

如需清空 ClickHouse 中已导入的 K 线数据（谨慎操作，不可逆）：

```bash
# 脚本方式（推荐，含安全确认）
./clear_database.sh

# 或直接运行 Go 程序
go run clear_database.go
```

清理后可按需重新初始化并导入数据：

```bash
go run cmd/main.go -cmd=init-db
go run cmd/main.go -cmd=run
```

## 快速测试

如果你想快速验证系统功能而不需要安装Docker和ClickHouse，可以使用简化测试：

```bash
# 运行简化测试（只测试数据下载和解析）
go run test/btc_test_simple.go
```

这个测试会：
- 下载币安历史数据
- 解析CSV格式的K线数据
- 验证数据完整性
- 显示示例数据

详细的测试说明请参考 [test/README.md](test/README.md)

## 使用指南

### 命令行选项

```bash
使用方法: ./binance-data-loader [选项]

命令:
  run           - 运行数据加载器 (默认)
  validate      - 验证现有数据
  verify-data   - 综合数据验证 (完整性+质量检查)
  init-db       - 初始化数据库表
  create-views  - 创建物化视图
  populate-views - 填充物化视图历史数据
  status        - 显示下载状态
  discover      - 发现交易对时间线
  update-latest - 更新到最新数据
  range-query   - 查询历史数据范围
  list-symbols  - 列出数据库中的交易对
  update-ranges - 更新交易对时间范围

选项:
  -config string
        配置文件路径 (默认 "config.yml")
  -cmd string
        要执行的命令 (默认 "run")
  -symbols string
        要处理的交易对列表，逗号分隔 (可选)
  -end string
        结束日期 (YYYY-MM-DD)
  -output string
        range-query 结果输出文件路径 (可选)
  -detailed
        显示详细状态信息
  -verbose
        启用详细日志
  -version
        显示版本信息
```

### 命令详细说明

#### 核心命令

**`run` - 运行数据加载器**
```bash
# 运行默认配置的数据加载
go run cmd/main.go -cmd=run

# 加载指定交易对的数据
go run cmd/main.go -cmd=run -symbols=BTCUSDT,ETHUSDT

# 加载到指定结束日期
go run cmd/main.go -cmd=run -end=2024-01-31
```

**`init-db` - 初始化数据库**
```bash
# 创建所有必要的数据库表和结构
go run cmd/main.go -cmd=init-db
```

**`create-views` - 创建物化视图**
```bash
# 创建5m、15m、1h、4h、1d等时间周期的物化视图
go run cmd/main.go -cmd=create-views -config=configs/config-dev.yml
```

**`populate-views` - 填充物化视图历史数据**
```bash
# 填充所有物化视图的历史数据（支持大数据量分批处理）
go run cmd/main.go -cmd=populate-views -config=configs/config-dev.yml
```

**populate-views功能特性**：
- 🔄 **智能分批**: 按月份自动分批处理，避免内存溢出
- 📊 **实时进度**: 显示详细的处理进度和批次信息
- ⚡ **性能优化**: 自动设置ClickHouse参数，支持大量交易对
- 🛡️ **数据一致**: 使用与物化视图相同的聚合逻辑
- 🔄 **断点续传**: 支持中断后继续处理，已处理数据不重复

**注意事项**：
- ClickHouse物化视图只处理创建后插入的新数据
- 历史数据需要使用此命令手动填充
- 建议在数据库负载较低时运行此命令

#### 数据管理命令

**`discover` - 发现交易对时间线**
```bash
# 发现所有USDT交易对的完整时间线信息
go run cmd/main.go -cmd=discover

# 发现特定交易对的时间线
go run cmd/main.go -cmd=discover -symbols=BTCUSDT,ETHUSDT

# 显示详细信息
go run cmd/main.go -cmd=discover -detailed
```

**`update-ranges` - 更新交易对时间范围**
```bash
# 更新所有交易对的时间范围信息
go run cmd/main.go -cmd=update-ranges

# 更新特定交易对的时间范围
go run cmd/main.go -cmd=update-ranges -symbols=BTCUSDT,ETHUSDT
```

**`list-symbols` - 列出交易对信息**
```bash
# 显示数据库中所有交易对的详细信息
go run cmd/main.go -cmd=list-symbols
```

#### 查询和分析命令

**`status` - 显示状态**
```bash
# 显示下载进度和状态概览
go run cmd/main.go -cmd=status

# 显示详细状态信息
go run cmd/main.go -cmd=status -detailed
```

**`range-query` - 查询数据范围**
```bash
# 查询所有交易对的历史数据范围
go run cmd/main.go -cmd=range-query

# 查询特定交易对并输出到文件
go run cmd/main.go -cmd=range-query -symbols=BTCUSDT,ETHUSDT -output=ranges.txt

# 查询所有交易对并保存结果
go run cmd/main.go -cmd=range-query -output=all_ranges.txt
```

#### 维护命令

**`validate` - 验证数据**
```bash
# 验证现有数据的完整性
go run cmd/main.go -cmd=validate

# 验证特定交易对的数据
go run cmd/main.go -cmd=validate -symbols=BTCUSDT,ETHUSDT
```

**`verify-data` - 综合数据验证**
```bash
# 对所有交易对进行综合数据验证（完整性+质量检查）
go run cmd/main.go -cmd=verify-data

# 验证特定交易对的数据完整性和质量
go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT,ETHUSDT

# 验证单个交易对
go run cmd/main.go -cmd=verify-data -symbols=BTCUSDT

# 验证指定交易对的数据（支持多个交易对）
go run cmd/main.go -cmd=verify-data -symbols=DFUSDT,ADAUSDT,LINKUSDT
```

`verify-data` 命令功能特性：
- **智能数据范围检测**: 自动分析数据库中的实际数据时间范围
- **月度完整性分析**: 逐月检查数据记录的完整性和连续性
- **数据质量评估**: 根据预期记录数计算完整性百分比
- **异常月份识别**: 自动识别缺失或不完整的月份数据
- **综合质量评分**: 提供基于完整性的数据质量等级评定
- **详细统计报告**: 展示每个交易对的详细数据统计信息
- **📄 自动报告生成**: 自动生成详细的Markdown格式验证报告文档

输出示例：
```
=== 批量数据完整性验证报告 ===
总交易对: 1, 已验证: 1
验证耗时: 0.80s
生成时间: 2025-09-02 21:27:22

完整性等级分布:
  完整 (95%+): 38 个月份
  部分 (0-95%): 0 个月份
  缺失 (0%): 0 个月份
平均完整性: 100.00%
总月份数: 38

各交易对完整性状况:
================================================================================
🟢 DFUSDT: 100.00% (优秀 (95%+))
  数据范围: 2021-11 - 2024-12 (38月份)
  月度统计: 完整 38, 部分 0, 缺失 0

📋 验证结论:
✅ 数据完整性优秀，无需特别关注
```

**验证指标说明**：
- **完整性等级**: 基于月度数据完整性的质量分级（优秀95%+, 良好80-95%, 一般60-80%, 较差<60%）
- **月度统计**: 每月数据记录数与预期记录数的比较分析
- **数据范围**: 交易对在数据库中的实际存储时间跨度
- **质量评分**: 综合所有月份数据得出的整体完整性百分比

**📄 报告文档**：
验证完成后，系统会自动生成详细的Markdown报告文档：
- **存储位置**: `reports/data-completeness-report-YYYYMMDD-HHMMSS.md`
- **报告内容**: 执行摘要、详细统计、问题分类、修复建议、质量等级说明
- **可视化分析**: 质量分布图、问题优先级、修复命令参考
- **便于分享**: Markdown格式，易于查看和分享给团队成员

**示例报告输出**：
```
📄 详细报告已生成: reports/data-completeness-report-20250902-220334.md
📊 验证了 1 个交易对，耗时 1.3s
🔴 0个严重问题，🟡 0个需关注，🟢 0个良好，✅ 1个优秀

🎉 所有交易对数据质量良好！
```

**`update-latest` - 更新到最新数据**
```bash
# 更新所有交易对到最新可用数据
go run cmd/main.go -cmd=update-latest

# 更新特定交易对到最新数据
go run cmd/main.go -cmd=update-latest -symbols=BTCUSDT
```

#### 数据导出命令

**`export-csv` - 导出CSV数据**
```bash
# 导出单个交易对的1分钟数据（默认）
go run cmd/main.go -cmd=export-csv -symbols=BTCUSDT

# 导出所有交易对的5分钟数据
go run cmd/main.go -cmd=export-csv -interval=5m -output=all_5m.csv

# 导出指定时间范围的数据
go run cmd/main.go -cmd=export-csv -symbols=ETHUSDT -start=2023-01-01 -end=2023-12-31

# 导出4小时数据到指定文件
go run cmd/main.go -cmd=export-csv -symbols=BTCUSDT -interval=4h -output=btc_4h.csv
```

**CSV导出功能特性**：
- **多时间间隔**: 支持 1m, 5m, 15m, 1h, 4h, 1d
- **灵活过滤**: 支持单个交易对或全部交易对
- **时间范围**: 可指定开始/结束日期，默认导出所有数据
- **高性能**: 流式处理，支持大数据量导出
- **标准格式**: ISO 8601时间戳 + 完整OHLCV数据
- **智能命名**: 自动生成文件名或用户自定义路径

**导出参数说明**：
- `-symbols`: 交易对名称（空值=全部）
- `-interval`: 时间间隔（默认1m）
- `-start`: 开始日期 (YYYY-MM-DD 或 YYYY-MM)
- `-end`: 结束日期 (YYYY-MM-DD 或 YYYY-MM)
- `-output`: 输出文件路径（空值=自动生成）

**CSV文件格式**：
```csv
timestamp,symbol,open,high,low,close,volume,quote_volume,trades,taker_buy_base_volume,taker_buy_quote_volume
2021-01-01T00:00:00Z,BTCUSDT,29374.99,29440.00,29350.00,29415.26,125.45,3689438.56,1205,58.23,1713847.23
```

📋 **详细使用指南**: 查看 [CSV导出功能使用指南](./docs/CSV_EXPORT_GUIDE.md)

### 配置说明

#### 数据库配置

```yaml
database:
  hosts: ["localhost:9000"]  # ClickHouse服务器地址
  database: "binance_data"    # 数据库名称
  username: "default"         # 用户名
  password: ""                # 密码
  dial_timeout: "10s"         # 连接超时
  max_open_conns: 10          # 最大连接数
  max_idle_conns: 5           # 最大空闲连接数
  conn_max_lifetime: "1h"     # 连接最大生存时间
  compression: "lz4"          # 压缩算法
```

#### 下载器配置

```yaml
downloader:
  concurrency: 5              # 并发下载数
  buffer_size: 1024           # 缓冲区大小
  user_agent: "BinanceDataLoader/1.0"
  compression: true           # 启用压缩
  max_file_size: 104857600    # 最大文件大小 (100MB)
```

#### 导入器配置

```yaml
importer:
  batch_size: 1000            # 批次大小
  buffer_size: 10000          # 缓冲区大小
  flush_interval: "30s"       # 刷新间隔
  deduplication: true         # 启用去重
```

### 监控和进度

启用监控后，可以通过以下方式查看进度：

1. **Web仪表板**: 访问 `http://localhost:8890`
2. **API接口**:
   - 总体进度: `GET /progress`
   - 详细进度: `GET /progress/detailed`
   - 交易对进度: `GET /progress/symbol/{symbol}`
   - 健康检查: `GET /health`

### 数据表结构

#### 主表 (klines_1m)

```sql
CREATE TABLE klines_1m (
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
```

#### 物化视图

系统会自动创建以下时间周期的物化视图：

- `klines_5m` - 5分钟K线
- `klines_15m` - 15分钟K线
- `klines_1h` - 1小时K线
- `klines_4h` - 4小时K线
- `klines_1d` - 1天K线

## 性能优化

### 下载性能

- 调整 `downloader.concurrency` 控制并发下载数
- 设置合适的 `downloader.buffer_size` 优化内存使用
- 启用压缩减少网络传输

### 导入性能

- 增加 `importer.batch_size` 提高批量插入效率
- 调整 `importer.buffer_size` 控制内存缓冲
- 设置合适的 `importer.flush_interval` 平衡性能和实时性

### 数据库性能

- 使用SSD存储提高I/O性能
- 调整ClickHouse的 `max_memory_usage` 设置
- 定期执行 `OPTIMIZE TABLE` 优化数据存储

## 故障排除

### 常见问题

1. **连接ClickHouse失败**
   - 检查数据库服务是否运行
   - 验证连接配置和凭据
   - 确认网络连接

2. **下载失败**
   - 检查网络连接
   - 验证币安数据源可访问性
   - 调整重试配置

3. **内存使用过高**
   - 减少并发数和缓冲区大小
   - 增加刷新频率
   - 监控系统资源使用

4. **数据验证失败**
   - 检查CSV文件格式
   - 验证数据完整性
   - 查看详细错误日志

5. **Ctrl+C无法停止程序**
   - 使用改进的停止脚本: `./stop.sh`
   - 检查是否有僵尸进程: `./stop.sh --dry-run`
   - 强制终止: `./stop.sh --force-timeout 5`

6. **进程无法正常启动**
   - 检查是否有残留进程: `./stop.sh --verbose`
   - 清理PID文件: `rm -f .data_loader_pid`
   - 使用测试模式验证: `./start.sh --test`

7. **脚本权限问题**
   - 添加执行权限: `chmod +x start.sh stop.sh`
   - 检查脚本依赖: `ls -la scripts/`

### 日志分析

系统提供详细的结构化日志：

```bash
# 查看错误日志
grep "level":"error" logs/app.log

# 查看性能指标
grep "performance" logs/app.log

# 查看数据质量报告
grep "data_quality" logs/app.log
```

## 开发指南

### 项目结构

```
.
├── cmd/                    # 应用程序入口
│   └── main.go
├── internal/               # 内部包
│   ├── config/            # 配置管理
│   ├── domain/            # 领域模型
│   ├── logger/            # 日志系统
│   └── state/             # 状态管理
├── pkg/                   # 公共包
│   ├── binance/           # 币安数据下载
│   ├── clickhouse/        # ClickHouse存储
│   ├── importer/          # 数据导入
│   ├── monitor/           # 监控报告
│   ├── parser/            # CSV解析
│   └── scheduler/         # 任务调度
├── config.yml             # 配置文件
├── go.mod                 # Go模块
└── README.md              # 说明文档
```

### 扩展开发

1. **添加新的数据源**
   - 实现 `domain.Downloader` 接口
   - 添加相应的配置选项

2. **支持新的存储后端**
   - 实现 `domain.KLineRepository` 接口
   - 添加相应的配置和初始化代码

3. **自定义数据处理**
   - 实现 `domain.Parser` 接口
   - 添加数据验证和转换逻辑

## 文档

### 详细文档

- 📖 [API文档](docs/API.md) - HTTP API接口说明
- 🛠️ [故障排除指南](docs/TROUBLESHOOTING.md) - 常见问题和解决方案
- 🔧 [信号处理机制](docs/SIGNAL_HANDLING.md) - 信号处理实现详解
- 🚀 [优化路线图](docs/OPTIMIZATION_ROADMAP.md) - 性能优化指南

### 快速链接

- [快速开始](#快速开始) - 快速部署和运行
- [命令详细说明](#命令详细说明) - 所有命令的使用方法
- [故障排除](#故障排除) - 常见问题解决
- [性能优化](#性能优化) - 系统调优建议

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request！

## 更新日志

### v1.2.2

- 🐛 **修复**: ClickHouse UInt64与int64类型不匹配导致的verify-data命令失败问题
- 🔧 **优化**: GetMonthlyDataStats和GetDataCompletenessForSymbol方法的数据类型处理
- ✅ **验证**: 修复后verify-data命令在多个交易对上运行稳定
- 🛠️ **改进**: 保持API接口兼容性的同时解决底层类型转换问题

### v1.2.1

- ✨ **新增**: `verify-data` 命令 - 综合数据验证功能
- 🔍 **增强**: 数据完整性和质量双重检查机制
- 📊 **新增**: 详细的数据质量统计报告
- 🛠️ **改进**: 异常数据检测和分析功能
- 📚 **更新**: README文档，添加数据验证使用指南

### v1.2.0

- 🛑 **重大改进**: 完全重构信号处理机制，解决Ctrl+C无法停止的问题
- 🔧 **新增**: 智能进程管理函数库 (`scripts/process_manager.sh`)
- ✨ **改进**: 启动脚本支持前台/后台/测试三种运行模式
- 🚀 **新增**: 停止脚本支持dry-run预览和详细输出模式
- 🔍 **增强**: 多种进程查找方式（PID文件、进程名、端口监听）
- ⚡ **优化**: 优雅关闭机制，支持自定义超时时间
- 🧪 **新增**: 完整的测试套件，覆盖脚本功能验证
- 📚 **完善**: 详细的故障排除指南和使用文档

### v1.1.0

- ✨ 新增 `update-ranges` 命令 - 更新交易对时间范围
- ✨ 新增 `list-symbols` 命令 - 列出数据库中的交易对信息
- ✨ 增强信号处理机制，支持更快的 Ctrl+C 响应
- 🔧 优化数据库清理脚本，保留 Docker 容器
- 📚 完善命令行帮助信息和文档

### v1.0.0

- 初始版本发布
- 支持币安K线数据下载
- ClickHouse数据存储
- 物化视图支持
- Web监控界面
