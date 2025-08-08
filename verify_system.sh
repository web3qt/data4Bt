#!/bin/bash

# 简单的系统验证脚本
# 验证 Binance Data Loader 系统的当前状态

echo "=== Binance Data Loader 系统验证 ==="
echo ""

# 1. 检查数据加载器进程
echo "1. 数据加载器状态:"
if [ -f ".data_loader_pid" ]; then
    pid=$(cat .data_loader_pid)
    if ps -p $pid > /dev/null 2>&1; then
        echo "   ✅ 正在运行 (PID: $pid)"
    else
        echo "   ❌ 进程不存在"
    fi
else
    echo "   ❌ 未启动"
fi
echo ""

# 2. 检查ClickHouse容器
echo "2. ClickHouse 容器状态:"
if docker ps | grep -q "shared-clickhouse"; then
    echo "   ✅ 容器运行中"
else
    echo "   ❌ 容器未运行"
fi
echo ""

# 3. 测试数据库连接
echo "3. 数据库连接测试:"
if docker exec shared-clickhouse clickhouse-client --database=data4BT --query='SELECT 1' > /dev/null 2>&1; then
    echo "   ✅ 连接正常"
else
    echo "   ❌ 连接失败"
fi
echo ""

# 4. 检查数据量
echo "4. 数据统计:"
if docker exec shared-clickhouse clickhouse-client --database=data4BT --query='SELECT 1' > /dev/null 2>&1; then
    klines_1m_count=$(docker exec shared-clickhouse clickhouse-client --database=data4BT --query='SELECT COUNT(*) FROM klines_1m' 2>/dev/null || echo "0")
    klines_5m_count=$(docker exec shared-clickhouse clickhouse-client --database=data4BT --query='SELECT COUNT(*) FROM klines_5m' 2>/dev/null || echo "0")
    echo "   📊 1分钟K线: $klines_1m_count 条记录"
    echo "   📊 5分钟K线: $klines_5m_count 条记录"
else
    echo "   ❌ 无法查询数据"
fi
echo ""

# 5. 检查日志文件
echo "5. 日志文件状态:"
if [ -f "logs/data_loader.log" ]; then
    log_lines=$(wc -l < logs/data_loader.log)
    log_size=$(du -h logs/data_loader.log | cut -f1)
    echo "   ✅ 日志文件存在: $log_lines 行, $log_size"
else
    echo "   ❌ 日志文件不存在"
fi
echo ""

echo "=== 验证完成 ==="
echo ""
echo "💡 管理命令:"
echo "   ./start.sh    - 启动数据加载器"
echo "   ./status.sh   - 查看详细状态"
echo "   ./stop.sh     - 停止数据加载器"
echo "   tail -f logs/data_loader.log - 查看实时日志"