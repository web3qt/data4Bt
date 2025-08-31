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

### 技术特性

- **并发处理**: 支持多协程并发下载和处理
- **内存优化**: 流式处理大文件，控制内存使用
- **错误恢复**: 自动重试和错误处理机制
- **配置灵活**: 支持YAML配置文件
- **监控友好**: 提供HTTP API和Web界面

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

复制并修改配置文件：

```bash
cp config.yml.example config.yml
```

主要配置项：

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

### 8. 创建物化视图

```bash
go run cmd/main.go -cmd=create-views
```

### 9. 定期维护

建议定期运行以下命令来维护数据的完整性：

```bash
# 每月更新交易对时间范围（当有新的月度数据时）
go run cmd/main.go -cmd=update-ranges

# 查看数据库中的交易对状态
go run cmd/main.go -cmd=list-symbols

# 检查下载状态和进度
go run cmd/main.go -cmd=status -detailed
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
  init-db       - 初始化数据库表
  create-views  - 创建物化视图
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
go run cmd/main.go -cmd=create-views
```

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

**`update-latest` - 更新到最新数据**
```bash
# 更新所有交易对到最新可用数据
go run cmd/main.go -cmd=update-latest

# 更新特定交易对到最新数据
go run cmd/main.go -cmd=update-latest -symbols=BTCUSDT
```

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

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request！

## 更新日志

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
