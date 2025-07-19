# BTC USDT 测试

这个测试程序用于验证项目的核心功能：下载、解析和存储BTCUSDT的1分钟K线数据到ClickHouse数据库。

## 前置条件

### 0. 安装Docker

如果系统没有安装Docker，请先安装：

**macOS:**
```bash
# 使用Homebrew安装
brew install --cask docker

# 或者从官网下载Docker Desktop
# https://www.docker.com/products/docker-desktop/
```

**启动Docker Desktop后，验证安装：**
```bash
docker --version
docker compose version
```

### 1. 启动ClickHouse数据库

在项目根目录运行以下命令启动ClickHouse：

```bash
# 启动ClickHouse容器
docker-compose up -d clickhouse

# 检查容器状态
docker-compose ps

# 查看日志
docker-compose logs clickhouse
```

### 2. 验证ClickHouse连接

```bash
# 使用ClickHouse客户端连接
docker exec -it data4bt-clickhouse clickhouse-client

# 或者通过HTTP接口测试
curl http://localhost:8123/ping
```

### 3. 可选：访问ClickHouse Web UI

启动Web UI（可选）：
```bash
docker-compose up -d clickhouse-ui
```

然后访问 http://localhost:8080 来查看ClickHouse的Web界面。

## 运行测试

### 简化测试（推荐）

如果您只想测试数据下载和解析功能（不需要数据库），可以运行简化版测试：

```bash
# 从项目根目录运行
go run test/btc_test_simple.go
```

这个测试会：
- 下载 BTCUSDT 历史数据（2023-07-15）
- 解析 CSV 数据
- 验证数据格式
- 显示示例数据

### 完整测试（需要 Docker）

如果您想测试完整的数据流程（包括数据库存储），需要先安装 Docker：

#### 前提条件

1. **安装 Docker**（如果尚未安装）：
   
   **使用 Homebrew**：
   ```bash
   brew install --cask docker
   ```
   
   **或下载 Docker Desktop**：
   访问 [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop) 下载并安装
   
   **验证安装**：
   ```bash
   docker --version
   docker-compose --version
   ```

2. **启动 ClickHouse Docker 容器**：
   ```bash
   cd docker
   docker-compose up -d clickhouse
   ```

3. **验证 ClickHouse 连接**：
   ```bash
   # 检查容器状态
   docker ps
   
   # 测试连接
   docker exec -it clickhouse-container clickhouse-client --query "SELECT 1"
   ```

4. **（可选）访问 ClickHouse Web UI**：
   打开浏览器访问 `http://localhost:8123/play`

#### 运行完整测试

```bash
# 从项目根目录运行
go run test/btc_test.go
```

## 测试流程

测试程序将执行以下步骤：

1. **加载配置** - 从 `config_test.yml` 加载测试配置
2. **初始化日志** - 设置结构化日志输出
3. **连接数据库** - 连接到ClickHouse数据库
4. **创建表结构** - 创建必要的数据表
5. **下载数据** - 从币安下载BTCUSDT昨天的1分钟K线数据
6. **解析数据** - 解析ZIP文件中的CSV数据
7. **验证数据** - 验证解析后的数据格式和完整性
8. **存储数据** - 将数据批量插入ClickHouse
9. **验证存储** - 验证数据是否正确存储

## 预期输出

### 简化测试输出

简化测试成功时，您应该看到类似以下的日志输出：

```json
{"level":"info","component":"btc-simple-test","time":"2025-07-19T11:39:20+08:00","message":"Starting BTC USDT simple test (download and parse only)"}
{"level":"info","component":"btc-simple-test","symbol":"BTCUSDT","date":"2023-07-15","time":"2025-07-19T11:39:20+08:00","message":"Testing data download and parsing"}
{"level":"info","component":"btc-simple-test","url":"https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2023-07-15.zip","time":"2025-07-19T11:39:20+08:00","message":"Downloading data"}
{"level":"info","component":"btc-simple-test","data_size":123456,"time":"2025-07-19T11:39:25+08:00","message":"Data downloaded successfully"}
{"level":"info","component":"btc-simple-test","total_klines":1440,"validation_valid":true,"validation_errors":0,"time":"2025-07-19T11:40:00+08:00","message":"Data parsed successfully"}
{"level":"info","component":"btc-simple-test","symbol":"BTCUSDT","open_time":"2023-07-15T08:00:00+08:00","open_price":30312,"time":"2025-07-19T11:40:00+08:00","message":"Record 1"}
{"level":"info","component":"btc-simple-test","time":"2025-07-19T11:40:00+08:00","message":"BTC USDT simple test completed successfully"}
```

### 完整测试输出

完整测试成功时，您应该看到类似以下的日志输出：

```json
{"level":"info","time":"2024-01-01T12:00:00Z","component":"btc-test","message":"Starting BTC USDT test"}
{"level":"info","time":"2024-01-01T12:00:01Z","component":"btc-test","message":"Creating database tables"}
{"level":"info","time":"2024-01-01T12:00:02Z","component":"btc-test","message":"Tables created successfully"}
{"level":"info","time":"2024-01-01T12:00:03Z","component":"btc-test","symbol":"BTCUSDT","date":"2024-01-01","message":"Testing data download and parsing"}
{"level":"info","time":"2024-01-01T12:00:04Z","component":"btc-test","url":"https://data.binance.vision/data/spot/daily/klines/BTCUSDT/BTCUSDT-1m-2024-01-01.zip","message":"Downloading data"}
{"level":"info","time":"2024-01-01T12:00:10Z","component":"btc-test","data_size":12345678,"message":"Data downloaded successfully"}
{"level":"info","time":"2024-01-01T12:00:11Z","component":"btc-test","message":"Parsing data"}
{"level":"info","time":"2024-01-01T12:00:12Z","component":"btc-test","total_klines":1440,"validation_valid":true,"validation_errors":0,"message":"Data parsed successfully"}
{"level":"info","time":"2024-01-01T12:00:13Z","component":"btc-test","message":"Saving data to ClickHouse"}
{"level":"info","time":"2024-01-01T12:00:15Z","component":"btc-test","saved_records":1440,"message":"Data saved successfully"}
{"level":"info","time":"2024-01-01T12:00:16Z","component":"btc-test","last_date":"2024-01-01","message":"Last date in database"}
{"level":"info","time":"2024-01-01T12:00:17Z","component":"btc-test","total_rows":1440,"valid_rows":1440,"is_valid":true,"message":"Data validation completed"}
{"level":"info","time":"2024-01-01T12:00:18Z","component":"btc-test","message":"BTC USDT test completed successfully"}
```

## 故障排除

### ClickHouse连接失败

如果遇到连接错误：

1. 确保Docker容器正在运行：`docker-compose ps`
2. 检查端口是否被占用：`lsof -i :9000`
3. 查看ClickHouse日志：`docker-compose logs clickhouse`

### 下载失败

如果数据下载失败：

1. 检查网络连接
2. 验证币安数据URL是否可访问
3. 确认日期格式正确（测试使用昨天的日期）

### 解析错误

如果数据解析失败：

1. 检查下载的数据是否完整
2. 验证CSV格式是否符合预期
3. 查看详细的错误日志

## 清理

测试完成后，可以停止并清理Docker容器：

```bash
# 停止容器
docker-compose down

# 删除数据卷（可选，会删除所有数据）
docker-compose down -v
```

## 配置说明

测试使用 `config_test.yml` 配置文件，主要配置项：

- **ClickHouse连接**：localhost:9000，数据库名为 `crypto`
- **下载设置**：单线程下载，重试3次
- **解析设置**：启用数据验证，最大文件大小100MB
- **日志设置**：JSON格式，INFO级别

可以根据需要修改配置文件来调整测试行为。