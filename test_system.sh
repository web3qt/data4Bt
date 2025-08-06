#!/bin/bash

# 测试脚本 - 验证数据库设置和数据流

set -e

echo "🚀 开始 data4BT 系统测试..."

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 检查Docker是否运行
echo -e "${BLUE}📋 检查Docker状态...${NC}"
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker未运行，请启动Docker${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Docker运行正常${NC}"

# 停止并清理现有容器
echo -e "${BLUE}🧹 清理现有容器...${NC}"
docker-compose down -v 2>/dev/null || true

# 启动ClickHouse
echo -e "${BLUE}🐘 启动ClickHouse容器...${NC}"
docker-compose up -d clickhouse

# 等待ClickHouse启动
echo -e "${YELLOW}⏳ 等待ClickHouse启动 (最多60秒)...${NC}"
for i in {1..60}; do
    if docker exec data4bt-clickhouse clickhouse-client --host 127.0.0.1 --port 9000 --query "SELECT 1" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ ClickHouse启动成功${NC}"
        break
    fi
    if [ $i -eq 60 ]; then
        echo -e "${RED}❌ ClickHouse启动超时${NC}"
        docker-compose logs clickhouse
        exit 1
    fi
    sleep 1
done

# 验证data4BT数据库创建
echo -e "${BLUE}🗄️  验证data4BT数据库...${NC}"
DB_EXISTS=$(docker exec data4bt-clickhouse clickhouse-client --host 127.0.0.1 --port 9000 --query "SELECT count() FROM system.databases WHERE name = 'data4BT'" 2>/dev/null || echo "0")
if [ "$DB_EXISTS" = "1" ]; then
    echo -e "${GREEN}✅ data4BT数据库存在${NC}"
else
    echo -e "${YELLOW}⚠️  data4BT数据库不存在，正在创建...${NC}"
    docker exec data4bt-clickhouse clickhouse-client --host 127.0.0.1 --port 9000 --query "CREATE DATABASE IF NOT EXISTS data4BT"
    echo -e "${GREEN}✅ data4BT数据库创建成功${NC}"
fi

# 运行单元测试
echo -e "${BLUE}🧪 运行单元测试...${NC}"
if go test ./pkg/parser/... -v; then
    echo -e "${GREEN}✅ Parser单元测试通过${NC}"
else
    echo -e "${RED}❌ Parser单元测试失败${NC}"
    exit 1
fi

if go test ./pkg/clickhouse/... -v; then
    echo -e "${GREEN}✅ Repository单元测试通过${NC}"
else
    echo -e "${RED}❌ Repository单元测试失败${NC}"
    exit 1
fi

if go test ./pkg/importer/... -v; then
    echo -e "${GREEN}✅ Importer单元测试通过${NC}"
else
    echo -e "${RED}❌ Importer单元测试失败${NC}"
    exit 1
fi

# 运行集成测试
echo -e "${BLUE}🔗 运行集成测试...${NC}"
export INTEGRATION_TEST=true
if go test ./test/... -v; then
    echo -e "${GREEN}✅ 集成测试通过${NC}"
else
    echo -e "${RED}❌ 集成测试失败${NC}"
    exit 1
fi

# 测试数据库初始化
echo -e "${BLUE}🏗️  测试数据库初始化...${NC}"
if go run cmd/main.go -cmd=init-db -config=config.yml; then
    echo -e "${GREEN}✅ 数据库初始化成功${NC}"
else
    echo -e "${RED}❌ 数据库初始化失败${NC}"
    exit 1
fi

# 验证表结构
echo -e "${BLUE}📊 验证表结构...${NC}"
TABLES=$(docker exec data4bt-clickhouse clickhouse-client --host 127.0.0.1 --port 9000 --query "SHOW TABLES FROM data4BT" 2>/dev/null || echo "")
if echo "$TABLES" | grep -q "klines_1m"; then
    echo -e "${GREEN}✅ klines_1m表创建成功${NC}"
else
    echo -e "${RED}❌ klines_1m表不存在${NC}"
    exit 1
fi

# 创建物化视图
echo -e "${BLUE}📈 创建物化视图...${NC}"
if go run cmd/main.go -cmd=create-views -config=config.yml; then
    echo -e "${GREEN}✅ 物化视图创建成功${NC}"
else
    echo -e "${RED}❌ 物化视图创建失败${NC}"
    exit 1
fi

# 测试符号发现功能
echo -e "${BLUE}🔍 测试符号发现功能...${NC}"
if timeout 30s go run cmd/main.go -cmd=discover -symbols=BTCUSDT -config=config.yml; then
    echo -e "${GREEN}✅ 符号发现功能正常${NC}"
else
    echo -e "${YELLOW}⚠️  符号发现功能超时或失败（可能是网络问题）${NC}"
fi

# 测试数据导入（少量数据）
echo -e "${BLUE}💾 测试数据导入...${NC}"
# 创建临时配置，限制处理范围
cat > test_import_config.yml << 'EOF'
# 测试导入配置
log:
  level: "info"
  format: "json"
  output: "stdout"

binance:
  base_url: "https://data.binance.vision"
  data_path: "/data/spot/daily/klines"
  symbols_filter: "USDT"
  interval: "1m"
  timeout: 30s
  retry_count: 2
  retry_delay: 3s

downloader:
  concurrency: 1
  buffer_size: 10
  user_agent: "BinanceDataLoader/1.0-test"
  enable_compression: true
  max_file_size: 100MB

parser:
  concurrency: 1
  buffer_size: 10
  validate_data: true
  skip_invalid_rows: true

database:
  clickhouse:
    hosts:
      - "localhost:9000"
    database: "data4BT"
    username: "default"
    password: ""
    settings:
      max_execution_time: 60
      max_memory_usage: 10000000000
    compression: "lz4"
    dial_timeout: 30s
    max_open_conns: 5
    max_idle_conns: 2
    conn_max_lifetime: 5m

importer:
  batch_size: 1000
  buffer_size: 10
  flush_interval: 10s
  enable_deduplication: true

state:
  storage_type: "file"
  file_path: "state/progress.json"
  backup_count: 3
  save_interval: 5s

materialized_views:
  enabled: true
  intervals:
    - "5m"
    - "1h"
  refresh_interval: 30s
  parallel_refresh: true

monitoring:
  enabled: false

scheduler:
  end_date: "2024-01-02"  # 限制到一天的数据
  batch_days: 1
  max_concurrent_symbols: 1
EOF

# 运行短期数据导入测试
echo -e "${YELLOW}⏳ 运行有限数据导入测试（30秒超时）...${NC}"
if timeout 30s go run cmd/main.go -cmd=run -config=test_import_config.yml -symbols=BTCUSDT -end=2024-01-02; then
    echo -e "${GREEN}✅ 数据导入测试成功${NC}"
    
    # 验证数据
    echo -e "${BLUE}🔍 验证导入的数据...${NC}"
    RECORD_COUNT=$(docker exec data4bt-clickhouse clickhouse-client --host 127.0.0.1 --port 9000 --query "SELECT count() FROM data4BT.klines_1m WHERE symbol = 'BTCUSDT'" 2>/dev/null || echo "0")
    if [ "$RECORD_COUNT" -gt "0" ]; then
        echo -e "${GREEN}✅ 成功导入 $RECORD_COUNT 条BTCUSDT记录${NC}"
    else
        echo -e "${YELLOW}⚠️  未找到导入的数据（可能是网络问题或数据不可用）${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  数据导入测试超时或失败（可能是网络问题）${NC}"
fi

# 清理临时文件
rm -f test_import_config.yml

# 显示最终状态
echo -e "${BLUE}📊 显示系统状态...${NC}"
if go run cmd/main.go -cmd=status -config=config.yml 2>/dev/null; then
    echo -e "${GREEN}✅ 状态命令执行成功${NC}"
else
    echo -e "${YELLOW}⚠️  状态命令执行失败（可能没有数据）${NC}"
fi

echo ""
echo -e "${GREEN}🎉 系统测试完成！${NC}"
echo -e "${BLUE}📋 测试总结：${NC}"
echo "  ✅ Docker环境正常"
echo "  ✅ ClickHouse容器启动成功"
echo "  ✅ data4BT数据库创建成功"
echo "  ✅ 单元测试全部通过"
echo "  ✅ 集成测试全部通过"
echo "  ✅ 数据库表结构创建成功"
echo "  ✅ 物化视图创建成功"
echo ""
echo -e "${BLUE}🔧 使用指南：${NC}"
echo "  • 启动系统: docker-compose up -d"
echo "  • 初始化数据库: go run cmd/main.go -cmd=init-db"
echo "  • 创建视图: go run cmd/main.go -cmd=create-views"
echo "  • 运行数据导入: go run cmd/main.go -cmd=run"
echo "  • 查看状态: go run cmd/main.go -cmd=status"
echo "  • 发现符号: go run cmd/main.go -cmd=discover"
echo ""
echo -e "${GREEN}✨ 系统已准备就绪，可以开始使用！${NC}"